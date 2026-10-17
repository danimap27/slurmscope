"""Perfiles de clúster y configuración de acceso remoto.

Todos los nodos de la Red Española de Supercomputación (RES) consultados, y también
Hércules (CICA), usan SLURM. Lo que cambia de un centro a otro no es el planificador
sino sus reglas: si exigen ``--account``/``--qos``, el tamaño máximo de un job array,
cómo se carga el software y desde dónde se entra. Un perfil recoge esas reglas para
que la especificación no tenga que repetirlas, y :func:`probe_cluster` las lee del
propio clúster cuando un dato no se conoce de antemano.

Los perfiles viven en :mod:`slurmscope.clusters_data`, con la fuente de cada dato.
Lo que la documentación no dice se deja vacío a propósito: es preferible preguntar al
clúster con ``slurmscope probe`` que suponer un valor.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence

from .clusters_data import PROFILES, ClusterProfile

# Valor por defecto de SLURM para MaxArraySize (índices 0..1000). Se usa solo
# cuando ni el perfil ni la especificación dicen otra cosa.
SLURM_DEFAULT_MAX_ARRAY_SIZE = 1001


class ClusterConfigError(ValueError):
    """Configuración de clúster inválida."""


def get_profile(name: Optional[str]) -> ClusterProfile:
    key = (name or "generic").strip().lower()
    if key not in PROFILES:
        raise ClusterConfigError(
            f"perfil de clúster desconocido {name!r}; disponibles: "
            + ", ".join(sorted(PROFILES))
        )
    return PROFILES[key]


@dataclass
class ClusterConfig:
    """Sección ``cluster:`` de la especificación.

    ``host`` vacío significa que SlurmScope se ejecuta en el propio clúster (o que el
    backend es el simulador). Con ``host`` (un alias de ``~/.ssh/config`` o
    ``usuario@máquina``) todas las órdenes de SLURM se ejecutan por SSH y el proyecto
    se sincroniza con ``rsync`` en ``remote_dir``.
    """

    profile: str = "generic"
    host: Optional[str] = None
    remote_dir: Optional[str] = None
    max_array_size: Optional[int] = None
    ssh_options: List[str] = field(default_factory=list)
    push_exclude: List[str] = field(default_factory=list)
    # "res": las horas vienen de una actividad de la RES → partición/QoS RES del perfil
    allocation: Optional[str] = None

    KNOWN = ("profile", "host", "remote_dir", "max_array_size", "ssh_options",
             "push_exclude", "allocation")

    @classmethod
    def from_mapping(cls, data: Optional[Mapping[str, Any]]) -> "ClusterConfig":
        data = dict(data or {})
        unknown = set(data) - set(cls.KNOWN)
        if unknown:
            raise ClusterConfigError(
                "opciones de cluster desconocidas: " + ", ".join(sorted(unknown))
            )
        profile = str(data.get("profile") or "generic")
        get_profile(profile)  # valida el nombre

        max_array = data.get("max_array_size")
        if max_array is not None:
            try:
                max_array = int(max_array)
            except (TypeError, ValueError):
                raise ClusterConfigError("cluster.max_array_size debe ser un entero") from None
            if max_array < 2:
                raise ClusterConfigError("cluster.max_array_size debe ser al menos 2")

        def _str_list(key: str) -> List[str]:
            value = data.get(key) or []
            if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
                raise ClusterConfigError(f"cluster.{key} debe ser una lista de cadenas")
            return list(value)

        host = data.get("host")
        if host is not None and (not isinstance(host, str) or not host.strip()):
            raise ClusterConfigError("cluster.host debe ser una cadena no vacía")
        if host and host.strip().startswith("-"):
            raise ClusterConfigError("cluster.host no puede empezar por '-'")
        remote_dir = data.get("remote_dir")
        if remote_dir is not None and not isinstance(remote_dir, str):
            raise ClusterConfigError("cluster.remote_dir debe ser una cadena")

        allocation = data.get("allocation")
        if allocation is not None:
            allocation = str(allocation).strip().lower()
            if allocation not in ("res", "centro"):
                raise ClusterConfigError("cluster.allocation debe ser 'res' o 'centro'")

        return cls(
            allocation=allocation,
            profile=profile,
            host=host.strip() if host else None,
            remote_dir=remote_dir,
            max_array_size=max_array,
            ssh_options=_str_list("ssh_options"),
            push_exclude=_str_list("push_exclude"),
        )

    @property
    def is_remote(self) -> bool:
        return bool(self.host)

    def effective_max_array_size(self) -> int:
        """Tamaño máximo del array: especificación > perfil > valor por defecto de SLURM."""
        if self.max_array_size:
            return self.max_array_size
        profile = get_profile(self.profile)
        return profile.max_array_size or SLURM_DEFAULT_MAX_ARRAY_SIZE

    def resolved_remote_dir(self, project: str) -> str:
        """Directorio remoto del proyecto (por defecto ``slurmscope/<proyecto>`` en $HOME)."""
        template = self.remote_dir or "slurmscope/{project}"
        try:
            path = template.format(project=project)
        except (KeyError, IndexError, ValueError):
            raise ClusterConfigError(
                "cluster.remote_dir solo admite el marcador {project}"
            ) from None
        path = path.strip()
        if not path or path in ("/", "~", "~/"):
            raise ClusterConfigError("cluster.remote_dir no puede ser la raíz ni $HOME")
        if ".." in path.split("/"):
            raise ClusterConfigError("cluster.remote_dir no puede contener '..'")
        if not re.match(r"^[A-Za-z0-9_./~${}-]+$", path):
            raise ClusterConfigError(
                "cluster.remote_dir solo admite letras, números, '_', '-', '.', '/', '~' "
                "y variables como $LUSTRE o $SCRATCH"
            )
        return path


# ───────────────────────── troceo de job arrays ──────────────────────────────

@dataclass(frozen=True)
class ArrayChunk:
    """Trozo de un barrido que cabe en un job array del clúster.

    Las tareas del trozo usan índices ``1..size``; la línea del fichero de comandos
    es ``SLURM_ARRAY_TASK_ID + offset``.
    """

    number: int
    offset: int
    size: int

    @property
    def first_run(self) -> int:
        return self.offset + 1

    @property
    def last_run(self) -> int:
        return self.offset + self.size

    def array_spec(self, max_parallel: int = 0) -> str:
        spec = f"1-{self.size}"
        if max_parallel and max_parallel > 0:
            spec += f"%{max_parallel}"
        return spec


def compress_indices(indices: Sequence[int]) -> str:
    """``[1, 2, 3, 7, 9, 10]`` → ``"1-3,7,9-10"`` (sintaxis de ``--array``)."""
    values = sorted(set(int(i) for i in indices))
    if not values:
        raise ValueError("no hay índices que enviar")
    parts: List[str] = []
    start = prev = values[0]
    for value in values[1:]:
        if value == prev + 1:
            prev = value
            continue
        parts.append(f"{start}-{prev}" if prev != start else str(start))
        start = prev = value
    parts.append(f"{start}-{prev}" if prev != start else str(start))
    return ",".join(parts)


def parse_array_indices(spec: str) -> List[int]:
    """``"1-3,7,9-12:2%4"`` → ``[1, 2, 3, 7, 9, 11]`` (ignora el ``%N``)."""
    text = (spec or "").split("%", 1)[0].strip()
    out: List[int] = []
    for part in filter(None, (p.strip() for p in text.split(","))):
        step = 1
        if ":" in part:
            part, step_text = part.split(":", 1)
            step = int(step_text)
        if "-" in part:
            first, last = (int(x) for x in part.split("-", 1))
            out.extend(range(first, last + 1, step))
        else:
            out.append(int(part))
    return sorted(set(out))


@dataclass(frozen=True)
class ArrayBatch:
    """Un envío: ejecuciones ``indices`` (1-based) que comparten ``offset``."""

    offset: int
    indices: tuple

    @property
    def tasks(self) -> List[int]:
        return [i - self.offset for i in self.indices]

    @property
    def size(self) -> int:
        return len(self.indices)

    def array_spec(self, max_parallel: int = 0) -> str:
        spec = compress_indices(self.tasks)
        if max_parallel and max_parallel > 0:
            spec += f"%{max_parallel}"
        return spec

    def describe(self) -> str:
        return compress_indices(self.indices)


def plan_batches(indices: Sequence[int], max_array_size: int) -> List[ArrayBatch]:
    """Agrupa ejecuciones arbitrarias en job arrays válidos para ``MaxArraySize``.

    Cada ventana ``[offset+1, offset+MaxArraySize-1]`` es un envío; dentro de ella,
    las tareas usan índices ``run - offset``, siempre menores que ``MaxArraySize``.
    Sirve igual para el envío inicial, para continuar por oleadas y para relanzar
    solo las ejecuciones fallidas.
    """
    per_window = max(1, int(max_array_size) - 1)
    windows: Dict[int, List[int]] = {}
    for index in sorted(set(int(i) for i in indices)):
        if index < 1:
            raise ValueError(f"índice de ejecución inválido: {index}")
        offset = ((index - 1) // per_window) * per_window
        windows.setdefault(offset, []).append(index)
    return [ArrayBatch(offset=o, indices=tuple(v)) for o, v in sorted(windows.items())]


def plan_chunks(n_runs: int, max_array_size: int) -> List[ArrayChunk]:
    """Divide ``n_runs`` en job arrays que respetan ``MaxArraySize``.

    SLURM acepta índices de ``0`` a ``MaxArraySize - 1``. Como las tareas se numeran
    desde 1, cada trozo puede tener como mucho ``MaxArraySize - 1`` tareas.
    """
    if n_runs < 0:
        raise ValueError("n_runs no puede ser negativo")
    per_chunk = max(1, int(max_array_size) - 1)
    chunks: List[ArrayChunk] = []
    offset = 0
    number = 1
    while offset < n_runs:
        size = min(per_chunk, n_runs - offset)
        chunks.append(ArrayChunk(number=number, offset=offset, size=size))
        offset += size
        number += 1
    return chunks


# ───────────────────────── sondeo del clúster ────────────────────────────────

@dataclass
class ProbeResult:
    """Lo que el clúster dice de sí mismo."""

    slurm_version: Optional[str] = None
    max_array_size: Optional[int] = None
    max_job_count: Optional[int] = None
    partitions: List[Dict[str, str]] = field(default_factory=list)
    associations: List[Dict[str, str]] = field(default_factory=list)
    qos: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    python: Optional[str] = None
    warnings: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "slurm_version": self.slurm_version,
            "max_array_size": self.max_array_size,
            "max_job_count": self.max_job_count,
            "partitions": self.partitions,
            "associations": self.associations,
            "qos": self.qos,
            "python": self.python,
            "warnings": self.warnings,
        }


_CONFIG_RE = re.compile(r"^\s*(\w+)\s*=\s*(.*?)\s*$")


def parse_scontrol_config(text: str) -> Dict[str, str]:
    values: Dict[str, str] = {}
    for line in text.splitlines():
        match = _CONFIG_RE.match(line)
        if match:
            values[match.group(1)] = match.group(2)
    return values


def parse_sinfo_summary(text: str) -> List[Dict[str, str]]:
    """Interpreta ``sinfo -h -o '%P|%a|%l|%F'``."""
    out: List[Dict[str, str]] = []
    for line in text.splitlines():
        parts = line.strip().split("|")
        if len(parts) < 4 or not parts[0]:
            continue
        name = parts[0]
        default = name.endswith("*")
        nodes = parts[3].split("/")
        out.append(
            {
                "name": name.rstrip("*"),
                "default": "sí" if default else "",
                "state": parts[1],
                "time_limit": parts[2],
                "idle_nodes": nodes[1] if len(nodes) == 4 else "",
                "total_nodes": nodes[3] if len(nodes) == 4 else "",
            }
        )
    return out


def parse_associations(text: str) -> List[Dict[str, str]]:
    """Interpreta ``sacctmgr -nP show assoc user=$USER format=account,partition,qos``."""
    out: List[Dict[str, str]] = []
    seen = set()
    for line in text.splitlines():
        parts = line.strip().split("|")
        if len(parts) < 3 or not parts[0]:
            continue
        key = tuple(parts[:3])
        if key in seen:
            continue
        seen.add(key)
        out.append({"account": parts[0], "partition": parts[1], "qos": parts[2]})
    return out


def parse_qos_table(text: str) -> Dict[str, Dict[str, Any]]:
    """Interpreta ``sacctmgr -nP show qos format=name,maxwall,maxjobspu,maxsubmitpu``."""
    out: Dict[str, Dict[str, Any]] = {}
    for line in text.splitlines():
        parts = line.strip().split("|")
        if len(parts) < 4 or not parts[0] or " " in parts[0]:
            continue
        def number(value: str) -> Optional[int]:
            return int(value) if value.strip().isdigit() else None
        out[parts[0]] = {"max_wall": parts[1] or None, "max_jobs": number(parts[2]),
                         "max_submit": number(parts[3])}
    return out


PROBE_SCRIPT = (
    "echo '##version'; sinfo --version 2>&1; "
    "echo '##config'; scontrol show config 2>&1 | grep -E '^(MaxArraySize|MaxJobCount) '; "
    "echo '##partitions'; sinfo -h -o '%P|%a|%l|%F' 2>&1; "
    "echo '##assoc'; sacctmgr -nP show assoc user=$USER format=account,partition,qos 2>&1; "
    "echo '##qos'; sacctmgr -nP show qos format=name,maxwall,maxjobspu,maxsubmitpu 2>&1; "
    "echo '##python'; python3 --version 2>&1; echo '##end'"
)


def parse_probe_output(text: str) -> ProbeResult:
    sections: Dict[str, List[str]] = {}
    current = None
    for line in text.splitlines():
        if line.startswith("##"):
            current = line[2:].strip()
            sections.setdefault(current, [])
            continue
        if current:
            sections[current].append(line)

    result = ProbeResult()
    version = "\n".join(sections.get("version", [])).strip()
    match = re.search(r"slurm\s+([\w.\-]+)", version)
    if match:
        result.slurm_version = match.group(1)
    elif "##version" in text:
        result.warnings.append("no se pudo leer la versión de SLURM (¿está en el PATH?)")

    config = parse_scontrol_config("\n".join(sections.get("config", [])))
    for key, attr in (("MaxArraySize", "max_array_size"), ("MaxJobCount", "max_job_count")):
        raw = config.get(key)
        if raw and raw.isdigit():
            setattr(result, attr, int(raw))
    if result.max_array_size is None:
        result.warnings.append("no se pudo leer MaxArraySize: se usará el del perfil")

    result.partitions = parse_sinfo_summary("\n".join(sections.get("partitions", [])))
    result.associations = parse_associations("\n".join(sections.get("assoc", [])))
    result.qos = parse_qos_table("\n".join(sections.get("qos", [])))
    python = "\n".join(sections.get("python", [])).strip()
    match = re.search(r"Python\s+([\d.]+)", python)
    if match:
        result.python = match.group(1)
    return result


def probe_cluster(run: Callable[[Sequence[str]], str]) -> ProbeResult:
    """Pregunta al clúster por sus límites con un solo viaje (una sola sesión SSH)."""
    return parse_probe_output(run(["bash", "-lc", PROBE_SCRIPT]))


def check_submission_rules(profile: ClusterProfile, account: Optional[str],
                           qos: Optional[str]) -> List[str]:
    """Errores de cuenta/QoS obligatorias (atajo; las normas completas están en rules)."""
    problems: List[str] = []
    if profile.requires_account and not account:
        problems.append(
            f"{profile.title} exige una cuenta: añade 'slurm.account' a la especificación"
        )
    if profile.requires_qos and not qos:
        hint = f" (p. ej. {', '.join(profile.qos_examples)})" if profile.qos_examples else ""
        problems.append(f"{profile.title} exige una QoS: añade 'slurm.qos'{hint}")
    return problems
