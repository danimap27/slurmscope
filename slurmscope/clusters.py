"""Perfiles de clúster y configuración de acceso remoto.

Todos los nodos de la Red Española de Supercomputación (RES) consultados, y también
Hércules (CICA), usan SLURM. Lo que cambia de un centro a otro no es el planificador
sino sus reglas: si exigen ``--account``/``--qos``, el tamaño máximo de un job array,
cómo se carga el software y desde dónde se entra. Un perfil recoge esas reglas para
que la especificación no tenga que repetirlas, y :func:`probe_cluster` las lee del
propio clúster cuando un dato no se conoce de antemano.

Solo se incluyen en los perfiles datos comprobados (en el clúster o en la guía
oficial del centro). Lo que no se ha podido comprobar se deja vacío a propósito: es
preferible preguntar al clúster con ``slurmscope probe`` que suponer un valor.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence

# Valor por defecto de SLURM para MaxArraySize (índices 0..1000). Se usa solo
# cuando ni el perfil ni la especificación dicen otra cosa.
SLURM_DEFAULT_MAX_ARRAY_SIZE = 1001


class ClusterConfigError(ValueError):
    """Configuración de clúster inválida."""


@dataclass(frozen=True)
class ClusterProfile:
    """Reglas conocidas de un centro de supercomputación."""

    name: str
    title: str
    centre: str
    login: Optional[str] = None
    partition: Optional[str] = None
    max_array_size: Optional[int] = None
    requires_account: bool = False
    requires_qos: bool = False
    qos_examples: Sequence[str] = ()
    setup_hint: str = ""
    notes: Sequence[str] = ()
    docs: str = ""
    verified: str = ""

    def as_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "title": self.title,
            "centre": self.centre,
            "login": self.login,
            "partition": self.partition,
            "max_array_size": self.max_array_size,
            "requires_account": self.requires_account,
            "requires_qos": self.requires_qos,
            "qos_examples": list(self.qos_examples),
            "setup_hint": self.setup_hint,
            "notes": list(self.notes),
            "docs": self.docs,
            "verified": self.verified,
        }


PROFILES: Dict[str, ClusterProfile] = {
    "hercules": ClusterProfile(
        name="hercules",
        title="Hércules",
        centre="CICA (Sevilla)",
        login="login.spc.cica.es",
        partition="standard",
        max_array_size=500,
        setup_hint="module load Miniconda3 && source activate <entorno>",
        notes=(
            "Hace falta la VPN del CICA activa.",
            "Particiones: standard (por defecto) y gpu (A100 40 GB).",
            "El Python del nodo de login es 3.6: SlurmScope se ejecuta en tu máquina "
            "y habla con el clúster por SSH.",
            "No usar 'conda activate' en scripts: 'source activate' tras cargar el módulo.",
        ),
        docs="https://www.cica.es/wp-content/uploads/2025/11/"
        "Manual-de-usuario-para-Cl%C3%BAster-H%C3%A9rcules-CICA.pdf",
        verified="2026-10-17, scontrol show config (SLURM 22.05.2, MaxArraySize=500)",
    ),
    "marenostrum5-gpp": ClusterProfile(
        name="marenostrum5-gpp",
        title="MareNostrum 5 (GPP, CPU)",
        centre="BSC-CNS (Barcelona) · RES",
        login="glogin1.bsc.es",
        requires_account=True,
        requires_qos=True,
        qos_examples=("gp_resa", "gp_resb", "gp_resc", "gp_debug"),
        notes=(
            "Sin --account y --qos el envío se rechaza.",
            "La cuenta es el grupo Unix del proyecto: 'bsc_project list'.",
            "Las QoS disponibles se consultan con 'bsc_queues'.",
            "La partición la determina la QoS: no hace falta indicarla.",
        ),
        docs="https://www.bsc.es/supportkc/docs/MareNostrum5/slurm",
        verified="guía oficial del BSC (consultada 2026-10-17)",
    ),
    "marenostrum5-acc": ClusterProfile(
        name="marenostrum5-acc",
        title="MareNostrum 5 (ACC, GPU)",
        centre="BSC-CNS (Barcelona) · RES",
        login="alogin1.bsc.es",
        requires_account=True,
        requires_qos=True,
        qos_examples=("acc_resa", "acc_resb", "acc_resc", "acc_debug"),
        notes=(
            "Sin --account y --qos el envío se rechaza.",
            "Pide las GPU con extra_sbatch, p. ej. '#SBATCH --gres=gpu:1'.",
        ),
        docs="https://www.bsc.es/supportkc/docs/MareNostrum5/slurm",
        verified="guía oficial del BSC (consultada 2026-10-17)",
    ),
    "finisterrae3": ClusterProfile(
        name="finisterrae3",
        title="FinisTerrae III",
        centre="CESGA (Santiago) · RES",
        qos_examples=("class_a", "class_b", "class_c"),
        notes=(
            "La QoS se asigna automáticamente según los recursos pedidos: no la "
            "indiques salvo que tu proyecto RES lo requiera (class_a/b/c).",
            "Límites de tu cuenta: comando 'batchlim'.",
        ),
        docs="https://cesga-docs.gitlab.io/ft3-user-guide/batch_qos.html",
        verified="guía oficial del CESGA (consultada 2026-10-17)",
    ),
    "tirant": ClusterProfile(
        name="tirant",
        title="Tirant",
        centre="Universitat de València · RES",
        login="tirant.uv.es",
        notes=(
            "El SIUV anunció el cierre temporal del puerto 22 para accesos desde fuera "
            "de la UV (30-04-2026): comprueba el acceso antes de enviar.",
        ),
        docs="https://www.uv.es/tirant/Manuales/Tirant3_user_manual.pdf",
        verified="manual de usuario de Tirant 3 (consultado 2026-10-17)",
    ),
    "picasso": ClusterProfile(
        name="picasso",
        title="Picasso",
        centre="SCBI, Universidad de Málaga · RES",
        notes=("Configura el host en 'cluster.host' y ejecuta 'slurmscope probe'.",),
        docs="https://www.scbi.uma.es/web/resources/user_manual/",
    ),
    "calendula": ClusterProfile(
        name="calendula",
        title="Caléndula",
        centre="SCAYLE (León) · RES",
        notes=("Configura el host en 'cluster.host' y ejecuta 'slurmscope probe'.",),
    ),
    "generic": ClusterProfile(
        name="generic",
        title="SLURM genérico",
        centre="cualquier clúster SLURM",
        notes=("Sin reglas propias: usa 'slurmscope probe' para leer los límites.",),
    ),
}


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

    KNOWN = ("profile", "host", "remote_dir", "max_array_size", "ssh_options", "push_exclude")

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

        return cls(
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
    python: Optional[str] = None
    warnings: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "slurm_version": self.slurm_version,
            "max_array_size": self.max_array_size,
            "max_job_count": self.max_job_count,
            "partitions": self.partitions,
            "associations": self.associations,
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


PROBE_SCRIPT = (
    "echo '##version'; sinfo --version 2>&1; "
    "echo '##config'; scontrol show config 2>&1 | grep -E '^(MaxArraySize|MaxJobCount) '; "
    "echo '##partitions'; sinfo -h -o '%P|%a|%l|%F' 2>&1; "
    "echo '##assoc'; sacctmgr -nP show assoc user=$USER format=account,partition,qos 2>&1; "
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
    """Errores que el clúster daría al enviar, detectados antes de enviar."""
    problems: List[str] = []
    if profile.requires_account and not account:
        problems.append(
            f"{profile.title} exige una cuenta: añade 'slurm.account' a la especificación"
        )
    if profile.requires_qos and not qos:
        hint = f" (p. ej. {', '.join(profile.qos_examples)})" if profile.qos_examples else ""
        problems.append(f"{profile.title} exige una QoS: añade 'slurm.qos'{hint}")
    return problems
