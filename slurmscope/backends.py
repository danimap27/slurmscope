"""Backends de clúster: SLURM real y un simulador determinista.

Todo el software habla con el clúster a través de esta interfaz, lo que permite
ejecutarlo y probarlo sin acceso a un HPC (``--backend mock``) y mantener el mismo
registro de ejecuciones en ambos casos.
"""

from __future__ import annotations

import json
import random
import re
import shlex
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .ledger import COMPLETED, FAILED, PENDING, RUNNING, normalise_state

SLURM_TIME_RE = re.compile(
    r"^(?:(?P<days>\d+)-)?(?P<hours>\d+):(?P<minutes>\d{2}):(?P<seconds>\d{2})$"
)


def parse_slurm_elapsed(text: str) -> Optional[float]:
    """Convierte ``[[D-]HH:MM:SS]`` de SLURM a segundos."""
    if not text:
        return None
    match = SLURM_TIME_RE.match(text.strip())
    if not match:
        return None
    days = int(match.group("days") or 0)
    hours = int(match.group("hours") or 0)
    minutes = int(match.group("minutes") or 0)
    seconds = int(match.group("seconds") or 0)
    return float(days * 86400 + hours * 3600 + minutes * 60 + seconds)


@dataclass
class QueueEntry:
    """Una fila de ``squeue``/``sacct`` ya normalizada."""

    job_id: str
    array_job_id: Optional[str]
    task_index: Optional[int]
    name: str
    state: str
    seconds: Optional[float] = None
    exit_code: Optional[int] = None
    partition: str = ""
    detail: str = ""

    def as_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "array_job_id": self.array_job_id,
            "task_index": self.task_index,
            "state": self.state,
            "seconds": self.seconds,
            "exit_code": self.exit_code,
        }


def split_array_job_id(job_id: str) -> Tuple[Optional[str], Optional[int]]:
    """``12345_7`` → ``('12345', 7)``; ``12345`` → ``('12345', None)``."""
    text = (job_id or "").strip()
    if not text:
        return None, None
    if "." in text:  # pasos como 12345_7.batch → se ignoran
        text = text.split(".", 1)[0]
    if "_" in text:
        base, _, tail = text.partition("_")
        if tail.isdigit():
            return base, int(tail)
    return text, None


class BackendError(RuntimeError):
    """Fallo al hablar con el clúster."""


class BaseBackend:
    name = "base"

    def available(self) -> Tuple[bool, str]:
        return True, ""

    def submit(
        self,
        script: Path,
        n_tasks: int,
        job_name: str = "sweep",
        array: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> str:
        """Envía ``script`` como job array de ``n_tasks`` tareas.

        ``array`` sobrescribe la directiva ``--array`` del script (troceo) y ``env``
        exporta variables adicionales a las tareas (``SLURMSCOPE_OFFSET``).
        """
        raise NotImplementedError

    def queue(self) -> List[QueueEntry]:
        raise NotImplementedError

    def accounting(self, job_ids: Sequence[str]) -> List[QueueEntry]:
        raise NotImplementedError

    def cancel(self, job_id: str) -> None:
        raise NotImplementedError


class RealSlurm(BaseBackend):
    """Backend contra un clúster SLURM real (sbatch/squeue/sacct/scancel)."""

    name = "slurm"

    def __init__(self, user: Optional[str] = None, runner=None,
                 workdir: Optional[Path] = None):
        self.user = user
        self.workdir = Path(workdir) if workdir else None
        self._run = runner or self._run_command

    # ── utilidades ───────────────────────────────────────────────────────────
    def _run_command(self, args: Sequence[str]) -> str:
        try:
            proc = subprocess.run(
                list(args), capture_output=True, text=True, check=False, timeout=60,
                cwd=str(self.workdir) if self.workdir else None,
            )
        except FileNotFoundError as exc:
            raise BackendError(f"no encuentro el ejecutable {args[0]!r}") from exc
        except subprocess.TimeoutExpired as exc:
            raise BackendError(f"{args[0]} ha excedido el tiempo de espera") from exc
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "").strip()
            raise BackendError(f"{' '.join(args)} → {detail}")
        return proc.stdout

    def available(self) -> Tuple[bool, str]:
        missing = [tool for tool in ("sbatch", "squeue") if shutil.which(tool) is None]
        if missing:
            return False, "no encuentro en el PATH: " + ", ".join(missing)
        if self.user is None:
            import getpass

            self.user = getpass.getuser()
        return True, ""

    # ── interfaz ─────────────────────────────────────────────────────────────
    @staticmethod
    def sbatch_args(script: Path, array: Optional[str] = None,
                    env: Optional[Dict[str, str]] = None) -> List[str]:
        args = ["sbatch", "--parsable"]
        if array:
            args.append(f"--array={array}")
        if env:
            for key, value in env.items():
                if not re.match(r"^[A-Z_][A-Z0-9_]*$", key) or "," in str(value):
                    raise BackendError(f"variable de entorno no válida para sbatch: {key}")
            args.append("--export=ALL," + ",".join(f"{k}={v}" for k, v in env.items()))
        args.append(str(script))
        return args

    def submit(
        self,
        script: Path,
        n_tasks: int,
        job_name: str = "sweep",
        array: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> str:
        out = self._run(self.sbatch_args(script, array=array, env=env))
        job_id = out.strip().splitlines()[-1].split(";", 1)[0].strip()
        if not job_id:
            raise BackendError("sbatch no devolvió un identificador de job")
        return job_id

    def queue(self) -> List[QueueEntry]:
        fmt = "%i|%j|%T|%M|%P|%R"
        out = self._run(["squeue", "-u", str(self.user), "--noheader", "--format=" + fmt])
        entries: List[QueueEntry] = []
        for line in out.splitlines():
            parts = line.strip().split("|")
            if len(parts) < 6:
                continue
            raw_id, name, state, elapsed, partition, reason = parts[:6]
            array_job_id, task_index = split_array_job_id(raw_id)
            entries.append(
                QueueEntry(
                    job_id=raw_id,
                    array_job_id=array_job_id,
                    task_index=task_index,
                    name=name,
                    state=normalise_state(state),
                    seconds=parse_slurm_elapsed(elapsed),
                    partition=partition,
                    detail=reason,
                )
            )
        return entries

    def accounting(self, job_ids: Sequence[str]) -> List[QueueEntry]:
        if not job_ids:
            return []
        fmt = "JobID,JobName,State,ExitCode,Elapsed,Start,End"
        out = self._run(
            ["sacct", "-j", ",".join(str(j) for j in job_ids), "--noheader",
             "--parsable2", "--format=" + fmt]
        )
        entries: List[QueueEntry] = []
        for line in out.splitlines():
            parts = line.strip().split("|")
            if len(parts) < 5:
                continue
            raw_id, name, state, exit_code, elapsed = parts[:5]
            if "." in raw_id:  # pasos internos (.batch, .extern)
                continue
            array_job_id, task_index = split_array_job_id(raw_id)
            code: Optional[int] = None
            if exit_code and exit_code != "":
                head = exit_code.split(":", 1)[0]
                code = int(head) if head.isdigit() else None
            entries.append(
                QueueEntry(
                    job_id=raw_id,
                    array_job_id=array_job_id,
                    task_index=task_index,
                    name=name,
                    state=normalise_state(state),
                    seconds=parse_slurm_elapsed(elapsed),
                    exit_code=code,
                )
            )
        return entries

    def cancel(self, job_id: str) -> None:
        self._run(["scancel", str(job_id)])


class SSHTransport:
    """Ejecuta órdenes en el nodo de login de un clúster por SSH.

    No guarda contraseñas ni las pide: usa la configuración de ``~/.ssh/config``
    (alias, claves, ``ProxyJump``). ``BatchMode=yes`` hace que falle en vez de
    quedarse esperando una contraseña cuando la clave no está autorizada.
    """

    def __init__(self, host: str, remote_dir: Optional[str] = None,
                 ssh_options: Sequence[str] = (), timeout: int = 90,
                 runner=None):
        if not host or host.startswith("-"):
            raise BackendError(f"host SSH no válido: {host!r}")
        self.host = host
        self._remote_dir_raw = self._relative_to_home(remote_dir) if remote_dir else None
        self._remote_dir_resolved: Optional[str] = None
        self.ssh_options = list(ssh_options)
        self.timeout = timeout
        self._exec = runner or self._subprocess

    @property
    def remote_dir(self) -> Optional[str]:
        """Directorio remoto. Las variables del centro ($LUSTRE, $SCRATCH, $FSCRATCH…)
        se resuelven en el propio clúster la primera vez, con un shell de login."""
        raw = self._remote_dir_raw
        if not raw or "$" not in raw:
            return raw
        if self._remote_dir_resolved is None:
            if "'" in raw or '"' in raw or "`" in raw:
                raise BackendError("cluster.remote_dir contiene comillas")
            out = self._exec([*self.ssh_base(), f"bash -lc 'printf %s \"{raw}\"'"]).strip()
            if not out.startswith("/") or "$" in out:
                raise BackendError(
                    f"no pude resolver {raw!r} en {self.host} (¿existe esa variable?)"
                )
            self._remote_dir_resolved = out.rstrip("/")
        return self._remote_dir_resolved

    @staticmethod
    def _relative_to_home(path: str) -> str:
        # ssh y rsync arrancan en $HOME: «~/x» y «x» son lo mismo, y así se evita
        # que la tilde llegue entrecomillada (y sin expandir) al shell remoto.
        path = path.strip()
        if path.startswith("~/"):
            path = path[2:]
        return path.rstrip("/") or "."

    def ssh_base(self) -> List[str]:
        return ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=20",
                *self.ssh_options, self.host]

    def _subprocess(self, argv: Sequence[str], timeout: Optional[int] = None) -> str:
        try:
            proc = subprocess.run(list(argv), capture_output=True, text=True,
                                  check=False, timeout=timeout or self.timeout)
        except FileNotFoundError as exc:
            raise BackendError(f"no encuentro el ejecutable {argv[0]!r}") from exc
        except subprocess.TimeoutExpired as exc:
            raise BackendError(
                f"{argv[0]} hacia {self.host} ha excedido el tiempo de espera "
                "(¿VPN o red caídas?)"
            ) from exc
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "").strip().splitlines()
            tail = detail[-1] if detail else f"código {proc.returncode}"
            if proc.returncode == 255 and argv[0] == "ssh":
                raise BackendError(f"no se pudo conectar con {self.host}: {tail}")
            raise BackendError(f"{self.host}: {tail}")
        return proc.stdout

    def run(self, args: Sequence[str], in_project: bool = True) -> str:
        """Ejecuta ``args`` en el clúster (dentro del directorio del proyecto)."""
        command = " ".join(shlex.quote(str(a)) for a in args)
        if in_project and self.remote_dir:
            command = f"cd {shlex.quote(self.remote_dir)} && {command}"
        return self._exec([*self.ssh_base(), command])

    def _rsync_ssh(self) -> str:
        return " ".join(shlex.quote(x) for x in
                        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=20",
                         *self.ssh_options])

    def push(self, local_dir: Path, excludes: Sequence[str] = ()) -> str:
        """Sube el proyecto al clúster (sin borrar nada en el destino)."""
        if not self.remote_dir:
            raise BackendError("falta cluster.remote_dir para subir el proyecto")
        if shutil.which("rsync") is None:
            raise BackendError("no encuentro rsync en esta máquina")
        target = shlex.quote(self.remote_dir)
        self._exec([*self.ssh_base(),
                    f"mkdir -p {target} {target}/logs {target}/results"])
        argv = ["rsync", "-az", "--itemize-changes", "-e", self._rsync_ssh()]
        for pattern in (".slurmscope/", "results/", "reports/", "logs/", ".git/",
                        "__pycache__/", *excludes):
            argv += ["--exclude", pattern]
        argv += [str(Path(local_dir)) + "/", f"{self.host}:{self.remote_dir}/"]
        return self._exec(argv, timeout=max(self.timeout, 600))

    def pull(self, local_dir: Path, what: Sequence[str] = ("results",)) -> str:
        """Descarga del clúster los directorios indicados (resultados, logs)."""
        if not self.remote_dir:
            raise BackendError("falta cluster.remote_dir para descargar")
        out = []
        for name in what:
            if name not in ("results", "logs", "reports"):
                raise BackendError(f"solo se pueden descargar results, logs o reports: {name}")
            destination = Path(local_dir) / name
            destination.mkdir(parents=True, exist_ok=True)
            out.append(self._exec(
                ["rsync", "-az", "--itemize-changes", "-e", self._rsync_ssh(),
                 f"{self.host}:{self.remote_dir}/{name}/", str(destination) + "/"],
                timeout=max(self.timeout, 1800),
            ))
        return "".join(out)


class RemoteSlurm(RealSlurm):
    """SLURM de un clúster remoto, controlado por SSH desde esta máquina.

    En el clúster solo hacen falta bash y las órdenes de SLURM: SlurmScope, su libro
    y su panel viven en local. Así funciona también en clústeres cuyo Python del
    nodo de login es antiguo (Hércules: 3.6).
    """

    def __init__(self, transport: SSHTransport, user: Optional[str] = None):
        super().__init__(user=user, runner=transport.run)
        self.transport = transport
        self.name = f"ssh:{transport.host}"

    def available(self) -> Tuple[bool, str]:
        try:
            out = self.transport.run(
                ["bash", "-lc", "whoami; command -v sbatch squeue sacct scancel | wc -l"],
                in_project=False,
            )
        except BackendError as exc:
            return False, str(exc)
        lines = [line.strip() for line in out.splitlines() if line.strip()]
        if len(lines) < 2:
            return False, f"respuesta inesperada de {self.transport.host}"
        if self.user is None:
            self.user = lines[0]
        if lines[-1] != "4":
            return False, f"faltan órdenes de SLURM en {self.transport.host}"
        return True, ""

    def queue(self) -> List[QueueEntry]:
        if self.user is None:
            ok, reason = self.available()
            if not ok:
                raise BackendError(reason)
        return super().queue()



class MockSlurm(BaseBackend):
    """Simulador determinista de un clúster SLURM.

    Permite probar el ciclo completo (envío, cola, contabilidad, estimaciones)
    sin acceso a un HPC. El tiempo avanza solo cuando se llama a :meth:`advance`.
    """

    name = "mock"

    def __init__(
        self,
        state_path: Path | str,
        seed: int = 20261016,
        parallel: int = 4,
        min_seconds: float = 60.0,
        max_seconds: float = 900.0,
        failure_rate: float = 0.0,
    ):
        self.state_path = Path(state_path)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.seed = seed
        self.parallel = max(1, int(parallel))
        self.min_seconds = max(0.0, float(min_seconds))
        self.max_seconds = max(self.min_seconds, float(max_seconds))
        self.failure_rate = min(1.0, max(0.0, float(failure_rate)))
        self._state = self._load()

    # ── persistencia del clúster simulado ────────────────────────────────────
    def _load(self) -> Dict[str, Any]:
        if self.state_path.is_file():
            try:
                return json.loads(self.state_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                pass
        return {"next_id": 900001, "clock": 0.0, "jobs": {}}

    def _save(self) -> None:
        self.state_path.write_text(
            json.dumps(self._state, indent=2, sort_keys=True), encoding="utf-8"
        )

    def _entry(self, job_id: str, task: Dict[str, Any]) -> QueueEntry:
        base, index = split_array_job_id(job_id)
        return QueueEntry(
            job_id=job_id,
            array_job_id=base,
            task_index=task.get("index", index),
            name=task.get("name", "sweep"),
            state=task["state"],
            seconds=task.get("duration") if task["state"] in (COMPLETED, FAILED) else task.get("elapsed"),
            exit_code=task.get("exit_code"),
        )

    # ── interfaz ─────────────────────────────────────────────────────────────
    def available(self) -> Tuple[bool, str]:
        return True, ""

    def submit(
        self,
        script: Path,
        n_tasks: int,
        job_name: str = "sweep",
        array: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> str:
        job_id = str(self._state["next_id"])
        self._state["next_id"] += 1
        rng = random.Random(f"{self.seed}:{job_id}")
        tasks: Dict[str, Any] = {}
        if array:
            from .clusters import parse_array_indices

            indices = parse_array_indices(array)
        else:
            indices = list(range(1, int(n_tasks) + 1))
        for index in indices:
            duration = round(rng.uniform(self.min_seconds, self.max_seconds), 2)
            fails = rng.random() < self.failure_rate
            tasks[str(index)] = {
                "index": index,
                "name": job_name,
                "duration": duration,
                "elapsed": 0.0,
                "state": PENDING,
                "exit_code": None,
                "will_fail": fails,
                "started_at": None,
                "ended_at": None,
            }
        self._state["jobs"][job_id] = {
            "name": job_name,
            "script": str(script),
            "array": array,
            "env": dict(env or {}),
            "submitted_at": self._state["clock"],
            "tasks": tasks,
        }
        self._save()
        return job_id

    def queue(self) -> List[QueueEntry]:
        entries: List[QueueEntry] = []
        for job_id, job in self._state["jobs"].items():
            for index, task in job["tasks"].items():
                if task["state"] in (PENDING, RUNNING):
                    entries.append(self._entry(f"{job_id}_{index}", task))
        return sorted(entries, key=lambda e: (e.array_job_id or "", e.task_index or 0))

    def accounting(self, job_ids: Sequence[str]) -> List[QueueEntry]:
        wanted = {str(j) for j in job_ids} if job_ids else None
        entries: List[QueueEntry] = []
        for job_id, job in self._state["jobs"].items():
            if wanted and job_id not in wanted:
                continue
            for index, task in job["tasks"].items():
                entries.append(self._entry(f"{job_id}_{index}", task))
        return sorted(entries, key=lambda e: (e.array_job_id or "", e.task_index or 0))

    def cancel(self, job_id: str) -> None:
        job = self._state["jobs"].get(str(job_id))
        if not job:
            raise BackendError(f"el job {job_id} no existe en el clúster simulado")
        for task in job["tasks"].values():
            if task["state"] in (PENDING, RUNNING):
                task["state"] = "CANCELLED"
                task["exit_code"] = None
        self._save()

    # ── avance de la simulación ──────────────────────────────────────────────
    def advance(self, seconds: float) -> float:
        """Avanza el reloj simulado ``seconds`` y devuelve el tiempo consumido."""
        budget = float(seconds)
        consumed = 0.0
        while budget > 1e-9:
            running = [
                (job_id, index, task)
                for job_id, job in self._state["jobs"].items()
                for index, task in job["tasks"].items()
                if task["state"] == RUNNING
            ]
            pending = [
                (job_id, index, task)
                for job_id, job in self._state["jobs"].items()
                for index, task in job["tasks"].items()
                if task["state"] == PENDING
            ]
            for job_id, index, task in pending:
                if len(running) >= self.parallel:
                    break
                task["state"] = RUNNING
                task["started_at"] = self._state["clock"]
                running.append((job_id, index, task))
            if not running:
                break
            step = min(
                budget,
                min(task["duration"] - task["elapsed"] for _, _, task in running),
            )
            step = max(step, 0.0)
            if step <= 0:
                step = budget
            self._state["clock"] += step
            consumed += step
            budget -= step
            for job_id, index, task in running:
                task["elapsed"] = round(task["elapsed"] + step, 4)
                if task["elapsed"] >= task["duration"]:
                    task["state"] = FAILED if task.get("will_fail") else COMPLETED
                    task["exit_code"] = 1 if task.get("will_fail") else 0
                    task["ended_at"] = self._state["clock"]
        self._save()
        return consumed

    def pending_count(self) -> int:
        return sum(
            1
            for job in self._state["jobs"].values()
            for task in job["tasks"].values()
            if task["state"] in (PENDING, RUNNING)
        )

    def reset(self) -> None:
        self._state = {"next_id": 900001, "clock": 0.0, "jobs": {}}
        self._save()


def get_backend(
    name: str,
    *,
    user: Optional[str] = None,
    mock_state: Optional[Path] = None,
    parallel: int = 4,
    failure_rate: float = 0.0,
    seed: int = 20261016,
    min_seconds: float = 60.0,
    max_seconds: float = 900.0,
    cluster: Any = None,
    project: str = "sweep",
    workdir: Optional[Path] = None,
) -> BaseBackend:
    """Factoría de backends.

    * ``auto``: ``ssh`` si la especificación define ``cluster.host``; si no, ``slurm``.
    * ``slurm``: SLURM de la máquina actual (SlurmScope corre en el nodo de login).
    * ``ssh``: SLURM remoto por SSH, según ``cluster.host`` y ``cluster.remote_dir``.
    * ``mock``: simulador determinista, para probar sin clúster.

    Los parámetros del simulador se ignoran con los backends reales.
    """
    key = (name or "auto").strip().lower()
    if key == "auto":
        key = "ssh" if cluster is not None and getattr(cluster, "host", None) else "slurm"
    if key in {"slurm", "real", "local"}:
        return RealSlurm(user=user, workdir=workdir)
    if key == "ssh":
        if cluster is None or not getattr(cluster, "host", None):
            raise BackendError(
                "el backend ssh necesita 'cluster.host' en la especificación "
                "(un alias de ~/.ssh/config o usuario@máquina)"
            )
        transport = SSHTransport(
            cluster.host,
            remote_dir=cluster.resolved_remote_dir(project),
            ssh_options=cluster.ssh_options,
        )
        return RemoteSlurm(transport, user=user)
    if key == "mock":
        return MockSlurm(
            mock_state or Path(".slurmscope/mock_cluster.json"),
            seed=seed,
            parallel=parallel,
            failure_rate=failure_rate,
            min_seconds=min_seconds,
            max_seconds=max_seconds,
        )
    raise BackendError(
        f"backend desconocido {name!r}: usa auto, slurm, ssh o mock"
    )
