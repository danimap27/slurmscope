"""Comprobación de una especificación contra las normas de su centro.

Antes de enviar nada, :func:`check_spec` contrasta el barrido con lo que exige la
documentación del centro (perfil) y, si existe, con lo que el propio clúster ha
dicho de sí mismo (``.slurmscope/probe.json``, generado por ``slurmscope probe``).

Tres niveles:

* ``error``: el clúster rechazaría el envío o el trabajo moriría seguro (falta la
  cuenta obligatoria, ``--time`` mayor que el máximo de la QoS…). Bloquea ``submit``.
* ``aviso``: el envío entraría pero incumple una norma o una buena práctica
  documentada (paralelismo por encima de lo permitido, descargas sin internet…).
* ``info``: datos útiles para el envío (oleadas por ``MaxSubmit``, dónde trabajar…).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .clusters import get_profile
from .clusters_data import Limits
from .config import SweepSpec

ERROR = "error"
WARNING = "aviso"
INFO = "info"

_TIME_RE = re.compile(r"^(?:(\d+)-)?(\d+)(?::(\d+))?(?::(\d+))?$")
_NETWORK_RE = re.compile(
    r"\b(pip3?\s+install|conda\s+(install|create|env\s+create)|mamba\s+install|"
    r"wget|curl|git\s+(clone|pull|fetch)|apt(-get)?\s+install|huggingface-cli\s+download)\b"
)


@dataclass(frozen=True)
class Finding:
    level: str
    code: str
    message: str

    def as_dict(self) -> Dict[str, str]:
        return {"level": self.level, "code": self.code, "message": self.message}


def slurm_time_seconds(text: Optional[str]) -> Optional[int]:
    """``D-HH:MM:SS``, ``HH:MM:SS``, ``MM:SS``, ``MM``, ``D-HH`` y ``D-HH:MM`` → segundos."""
    if not text:
        return None
    value = str(text).strip()
    if value.upper() in ("UNLIMITED", "INFINITE"):
        return None
    match = _TIME_RE.match(value)
    if not match:
        return None
    days, a, b, c = match.groups()
    if days is not None:  # D-HH[:MM[:SS]]
        return int(days) * 86400 + int(a) * 3600 + int(b or 0) * 60 + int(c or 0)
    if b is None:          # MM
        return int(a) * 60
    if c is None:          # MM:SS
        return int(a) * 60 + int(b)
    return int(a) * 3600 + int(b) * 60 + int(c)


def load_probe(spec: SweepSpec) -> Optional[Dict[str, Any]]:
    path = spec.source.parent / ".slurmscope" / "probe.json"
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _gpus_requested(spec: SweepSpec) -> int:
    for line in spec.slurm.extra_sbatch or []:
        match = re.search(r"--gres=gpu(?::[A-Za-z0-9_]+)?:(\d+)", line)
        if match:
            return int(match.group(1))
        if re.search(r"--gres=gpu\b", line):
            return 1
        match = re.search(r"--gpus(?:-per-node|-per-task)?=(?:[A-Za-z0-9_]+:)?(\d+)", line)
        if match:
            return int(match.group(1))
    return 0


def effective_limits(spec: SweepSpec,
                     probe: Optional[Dict[str, Any]] = None) -> Tuple[Optional[str], Limits]:
    """Límites que se aplicarán al barrido: ``(nombre, Limits)``.

    Orden: QoS pedida → asignación automática por tiempo (FinisTerrae III) →
    partición → límite general del perfil. Lo que diga ``probe`` sobre una QoS manda
    sobre el perfil.
    """
    profile = spec.profile
    name: Optional[str] = None
    limits = Limits()
    qos = spec.effective_qos
    seconds = slurm_time_seconds(spec.slurm.time)

    if qos and qos in profile.qos_limits:
        name, limits = qos, profile.qos_limits[qos]
    elif qos is None and profile.time_tiers and seconds is not None:
        for tier in profile.time_tiers:
            tier_limits = profile.qos_limits.get(tier) or profile.partition_limits.get(tier)
            wall = slurm_time_seconds(tier_limits.max_wall) if tier_limits else None
            if tier_limits and wall is not None and seconds <= wall:
                name, limits = tier, tier_limits
                break
        else:
            last = profile.time_tiers[-1]
            name = last
            limits = profile.qos_limits.get(last) or profile.partition_limits.get(last) or Limits()
    elif spec.effective_partition and spec.effective_partition in profile.partition_limits:
        name = spec.effective_partition
        limits = profile.partition_limits[name]
    elif profile.max_wall:
        name, limits = None, Limits(max_wall=profile.max_wall)

    probed = (probe or {}).get("qos", {}).get(qos or name or "", None) if probe else None
    if probed:
        limits = Limits(
            max_wall=probed.get("max_wall") or limits.max_wall,
            max_jobs=probed.get("max_jobs") if probed.get("max_jobs") is not None else limits.max_jobs,
            max_submit=(probed.get("max_submit") if probed.get("max_submit") is not None
                        else limits.max_submit),
            note=limits.note,
        )
    return name, limits


def submit_capacity(spec: SweepSpec, probe: Optional[Dict[str, Any]] = None) -> Optional[int]:
    """Cuántas tareas pueden estar en cola a la vez (``MaxSubmit``), si se sabe."""
    _, limits = effective_limits(spec, probe)
    caps = [c for c in (limits.max_submit, spec.profile.max_submit_total) if c]
    return min(caps) if caps else None


def check_spec(spec: SweepSpec, n_runs: int,
               probe: Optional[Dict[str, Any]] = None) -> List[Finding]:
    profile = get_profile(spec.cluster.profile)
    opts = spec.slurm
    out: List[Finding] = []

    def add(level: str, code: str, message: str) -> None:
        out.append(Finding(level, code, message))

    # ── obligatorios del centro ──────────────────────────────────────────────
    if profile.requires_account and not opts.account:
        add(ERROR, "account", f"{profile.title} exige --account: añade 'slurm.account'.")
    if profile.requires_qos and not spec.effective_qos:
        examples = [q for q in profile.qos_examples if "res" in q] or list(profile.qos_examples)
        hint = f" (p. ej. {', '.join(examples[:4])})" if examples else ""
        add(ERROR, "qos", f"{profile.title} exige --qos: añade 'slurm.qos'{hint}.")
    if profile.requires_partition and not spec.effective_partition:
        add(ERROR, "partition", f"{profile.title} exige --partition: añade 'slurm.partition'.")
    if profile.qos_is_automatic and opts.qos and opts.qos not in profile.qos_limits:
        add(WARNING, "qos-auto",
            f"{profile.title} asigna la QoS automáticamente; '{opts.qos}' no es una QoS "
            "documentada y el envío puede fallar. Quítala salvo que el centro te la haya dado.")
    elif profile.qos_is_automatic and opts.qos:
        add(INFO, "qos-auto",
            f"{profile.title} asigna la QoS automáticamente: indica '{opts.qos}' solo si tu "
            "proyecto lo requiere.")
    if spec.cluster.allocation == "res" and not profile.res_node:
        add(WARNING, "res", f"{profile.title} no es un nodo de la RES.")
    if (spec.cluster.allocation != "res" and profile.res_partition
            and not opts.partition):
        add(INFO, "res",
            f"Si usas horas de la RES en {profile.title}, pon 'cluster.allocation: res' "
            f"(partición {profile.res_partition}"
            + (f", QoS {profile.res_qos}" if profile.res_qos and not profile.qos_is_automatic else "")
            + ").")

    # ── tiempo ───────────────────────────────────────────────────────────────
    seconds = slurm_time_seconds(opts.time)
    if seconds is None:
        add(ERROR, "time", f"slurm.time={opts.time!r} no es un tiempo de SLURM válido "
                           "(D-HH:MM:SS).")
    name, limits = effective_limits(spec, probe)
    wall = slurm_time_seconds(limits.max_wall)
    if seconds is not None and wall is not None and seconds > wall:
        where = f"la QoS/partición '{name}'" if name else profile.title
        add(ERROR, "time-limit",
            f"--time={opts.time} supera el máximo de {where} ({limits.max_wall}): SLURM "
            "rechazaría el trabajo o lo dejaría pendiente para siempre.")
    elif seconds is not None and wall is not None and seconds == wall:
        add(INFO, "time-limit",
            f"--time igual al máximo de '{name}' ({limits.max_wall}); un --time ajustado "
            "suele entrar antes en cola.")
    if seconds is not None and profile.name == "picasso" and seconds > 3 * 86400:
        add(WARNING, "time-risk", "Picasso considera arriesgado pedir más de 3 días.")

    # ── concurrencia y cola ──────────────────────────────────────────────────
    if limits.max_jobs and opts.max_parallel and opts.max_parallel > limits.max_jobs:
        add(WARNING, "parallel",
            f"max_parallel={opts.max_parallel} supera las {limits.max_jobs} tareas "
            f"simultáneas de '{name}': el resto esperará en cola igualmente.")
    if limits.max_jobs and not opts.max_parallel and n_runs > limits.max_jobs:
        add(INFO, "parallel",
            f"Como mucho {limits.max_jobs} tareas de '{name}' correrán a la vez; "
            "fija slurm.max_parallel para que las estimaciones sean realistas.")
    capacity = submit_capacity(spec, probe)
    if capacity and n_runs > capacity:
        add(INFO, "waves",
            f"{n_runs} ejecuciones > {capacity} trabajos en cola permitidos (cada tarea de "
            "un array cuenta): se enviará por oleadas. Lanza las siguientes con "
            "'slurmscope resume' cuando avance la cola.")
    if profile.support_review_above and n_runs > profile.support_review_above:
        add(WARNING, "support",
            f"{profile.title} pide avisar a soporte antes del primer array de más de "
            f"{profile.support_review_above} tareas.")
    if not opts.max_parallel and n_runs > 50:
        add(WARNING, "parallel",
            "Sin slurm.max_parallel (%N del array) todas las tareas compiten a la vez; "
            "los centros recomiendan limitar la concurrencia de los arrays.")

    # ── recursos ─────────────────────────────────────────────────────────────
    gpus = _gpus_requested(spec)
    if profile.cpus_per_gpu and gpus:
        needed = profile.cpus_per_gpu * gpus
        if opts.cpus_per_task < needed:
            add(ERROR, "cpus-per-gpu",
                f"{profile.title} exige {profile.cpus_per_gpu} CPU por GPU: con {gpus} GPU "
                f"pide cpus_per_task >= {needed} (ahora {opts.cpus_per_task}).")
    if not str(opts.mem or "").strip():
        add(ERROR, "mem", "slurm.mem vacío: varios centros (FinisTerrae III) lo exigen.")

    # ── software y red ───────────────────────────────────────────────────────
    if profile.internet_from_cluster is False and _NETWORK_RE.search(opts.setup or ""):
        add(ERROR, "internet",
            f"{profile.title} no permite conexiones salientes: el 'setup' descarga "
            "software y fallará en los nodos. Prepara el entorno antes y súbelo.")
    if profile.internet_from_cluster is False and _NETWORK_RE.search(spec.command):
        add(WARNING, "internet",
            f"El comando parece descargar algo y {profile.title} no tiene salida a internet.")
    if "conda activate" in (opts.setup or "") and profile.name == "hercules":
        add(WARNING, "conda",
            "En Hércules 'conda activate' falla en scripts: usa conda_env o "
            "'module load Miniconda3 && source activate <env>'.")

    # ── dónde se ejecuta ─────────────────────────────────────────────────────
    if spec.cluster.host and profile.home_for_jobs is False:
        remote = spec.cluster.resolved_remote_dir(spec.project)
        if not remote.startswith(("/", "$")) or remote.startswith(("~", "$HOME")):
            add(WARNING, "workdir",
                f"{profile.title} desaconseja ejecutar trabajos desde $HOME y cluster."
                f"remote_dir='{remote}' cae ahí. Usa {profile.workdir_hint}.")
    host = (spec.cluster.host or "").split("@")[-1]
    if host and "." in host and profile.logins and host not in profile.logins:
        add(INFO, "login",
            f"El host '{spec.cluster.host}' no es un login documentado de {profile.title} "
            f"({', '.join(profile.logins)}).")

    # ── arrays ───────────────────────────────────────────────────────────────
    max_array = spec.cluster.effective_max_array_size()
    probed_max = (probe or {}).get("max_array_size")
    if probed_max and probed_max < max_array:
        add(ERROR, "array-size",
            f"El clúster dice MaxArraySize={probed_max} y la especificación usa {max_array}: "
            f"pon 'cluster.max_array_size: {probed_max}'.")
    if not profile.max_array_size and not spec.cluster.max_array_size and not probed_max \
            and n_runs > 999:
        add(INFO, "array-size",
            f"{profile.title} no documenta MaxArraySize: ejecuta 'slurmscope probe' antes "
            "de enviar más de 1000 tareas.")
    return out


def has_errors(findings: List[Finding]) -> bool:
    return any(f.level == ERROR for f in findings)


def format_findings(findings: List[Finding]) -> str:
    if not findings:
        return "sin incidencias: la especificación cumple las normas conocidas del centro"
    order = {ERROR: 0, WARNING: 1, INFO: 2}
    lines = []
    for f in sorted(findings, key=lambda f: order.get(f.level, 9)):
        lines.append(f"[{f.level:5}] {f.message}")
    return "\n".join(lines)


def probe_path(spec: SweepSpec) -> Path:
    return spec.source.parent / ".slurmscope" / "probe.json"
