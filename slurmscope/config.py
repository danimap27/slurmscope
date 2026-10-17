"""Carga y validación de la especificación de un barrido de experimentos.

El fichero de especificación se escribe en YAML (si PyYAML está disponible) o en
JSON, y describe: qué parámetros forman el producto cartesiano, cómo se construye
el comando de cada ejecución, dónde se escriben los resultados y con qué opciones
se envía el job array a SLURM.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

from .clusters import ClusterConfig, ClusterConfigError, get_profile

DEFAULT_SPEC_NAMES = ("sweep.yaml", "sweep.yml", "sweep.json")


class ConfigError(ValueError):
    """Error de validación de la especificación."""


@dataclass
class SlurmOptions:
    """Opciones de envío del job array."""

    job_name: str = "sweep"
    # None: la del perfil de clúster, y si el perfil no fija ninguna, la partición
    # por defecto del propio clúster (no se escribe la directiva).
    partition: Optional[str] = None
    account: Optional[str] = None
    qos: Optional[str] = None
    time: str = "08:00:00"
    cpus_per_task: int = 4
    mem: str = "16G"
    max_parallel: int = 0
    log_dir: str = "logs"
    conda_base: Optional[str] = None
    conda_env: Optional[str] = None
    setup: str = ""
    extra_sbatch: List[str] = field(default_factory=list)
    shebang: str = "#!/bin/bash"

    @classmethod
    def from_mapping(cls, data: Optional[Mapping[str, Any]]) -> "SlurmOptions":
        data = data or {}
        known = {
            "job_name", "partition", "account", "qos", "time", "cpus_per_task",
            "mem", "max_parallel", "log_dir", "conda_base", "conda_env", "setup",
            "extra_sbatch", "shebang",
        }
        unknown = set(data) - known
        if unknown:
            raise ConfigError(
                "opciones de slurm desconocidas: " + ", ".join(sorted(unknown))
            )
        kwargs: Dict[str, Any] = {}
        for key in known:
            if key in data:
                kwargs[key] = data[key]
        if "cpus_per_task" in kwargs:
            kwargs["cpus_per_task"] = _as_int(kwargs["cpus_per_task"], "slurm.cpus_per_task")
        if "max_parallel" in kwargs:
            kwargs["max_parallel"] = _as_int(kwargs["max_parallel"], "slurm.max_parallel")
        for key in ("partition", "account", "qos"):
            value = kwargs.get(key)
            if value is None or value == "":
                kwargs[key] = None
                continue
            if not isinstance(value, str) or not _SAFE_DIRECTIVE.match(value):
                raise ConfigError(
                    f"slurm.{key} solo admite letras, números, '_', '-', '.' y ',' "
                    f"(recibido {value!r})"
                )
        extra = kwargs.get("extra_sbatch")
        if extra is not None:
            if not isinstance(extra, list) or not all(isinstance(x, str) for x in extra):
                raise ConfigError("slurm.extra_sbatch debe ser una lista de cadenas")
        return cls(**kwargs)


@dataclass
class SweepSpec:
    """Especificación completa de un barrido."""

    project: str
    command: str
    sweep: Dict[str, List[Any]]
    source: Path
    results_root: str = "results"
    run_id: str = "{index:04d}"
    create_run_dirs: bool = True
    slurm: SlurmOptions = field(default_factory=SlurmOptions)
    cluster: ClusterConfig = field(default_factory=ClusterConfig)
    raw: Dict[str, Any] = field(default_factory=dict)

    @property
    def effective_partition(self) -> Optional[str]:
        """Partición a pedir: la de la especificación o, si no hay, la del perfil."""
        if self.slurm.partition:
            return self.slurm.partition
        return get_profile(self.cluster.profile).partition

    @property
    def n_runs(self) -> int:
        total = 1
        for values in self.sweep.values():
            total *= len(values)
        return total

    @property
    def params(self) -> List[str]:
        """Nombres de los parámetros del barrido, en el orden declarado."""
        return list(self.sweep)

    def results_path(self) -> Path:
        """``results_root`` resuelto relativo al directorio de la especificación."""
        root = Path(self.results_root)
        if root.is_absolute():
            return root
        return (self.source.parent / root).resolve()

    def resolve(self, relative: str) -> Path:
        """Resuelve una ruta relativa al directorio de la especificación."""
        path = Path(relative)
        if path.is_absolute():
            return path
        return (self.source.parent / path).resolve()


_SAFE_DIRECTIVE = re.compile(r"^[A-Za-z0-9_.,\-]+$")


def _as_int(value: Any, where: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ConfigError(f"{where} debe ser un entero, recibido {value!r}") from None


def find_spec(start: Optional[Path] = None) -> Path:
    """Busca la especificación en ``start`` (o el directorio actual)."""
    base = Path(start or Path.cwd())
    for name in DEFAULT_SPEC_NAMES:
        candidate = base / name
        if candidate.is_file():
            return candidate
    raise ConfigError(
        "no se encontró ninguna especificación "
        f"({', '.join(DEFAULT_SPEC_NAMES)}) en {base}"
    )


def _read_mapping(path: Path) -> Dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    suffix = path.suffix.lower()
    if suffix == ".json":
        return _expect_mapping(json.loads(text), path)
    if suffix in (".yaml", ".yml"):
        try:
            import yaml  # type: ignore
        except ImportError:
            # Sin PyYAML se usa el lector propio: cero dependencias.
            from .miniyaml import MiniYamlError, loads

            try:
                return _expect_mapping(loads(text), path)
            except MiniYamlError as exc:
                raise ConfigError(
                    f"no pude interpretar {path.name} con el lector YAML incluido "
                    f"({exc}); instala PyYAML (slurmscope[yaml]) si usas YAML completo"
                ) from None
        return _expect_mapping(yaml.safe_load(text), path)
    raise ConfigError(f"extensión de especificación no soportada: {path.suffix!r}")


def _expect_mapping(data: Any, path: Path) -> Dict[str, Any]:
    if data is None:
        raise ConfigError(f"la especificación {path} está vacía")
    if not isinstance(data, dict):
        raise ConfigError(f"la especificación {path} debe ser un objeto/mapa")
    return data


def load_spec(path: Optional[Path] = None) -> SweepSpec:
    """Carga y valida una especificación desde disco."""
    spec_path = Path(path) if path else find_spec()
    if spec_path.is_dir():
        spec_path = find_spec(spec_path)
    if not spec_path.is_file():
        raise ConfigError(f"no existe la especificación {spec_path}")

    data = _read_mapping(spec_path)

    project = data.get("project")
    if not project or not isinstance(project, str):
        raise ConfigError("falta el campo obligatorio 'project' (cadena)")

    command = data.get("command")
    if not command or not isinstance(command, str):
        raise ConfigError(
            "falta el campo obligatorio 'command' (plantilla del comando de cada ejecución)"
        )

    sweep = data.get("sweep")
    if not isinstance(sweep, dict) or not sweep:
        raise ConfigError("'sweep' debe ser un mapa no vacío de parámetro -> lista de valores")

    normalised: Dict[str, List[Any]] = {}
    for name, values in sweep.items():
        if not isinstance(name, str):
            raise ConfigError(f"nombre de parámetro inválido en 'sweep': {name!r}")
        if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
            raise ConfigError(
                f"los valores de '{name}' deben ser una lista, recibido {type(values).__name__}"
            )
        if len(values) == 0:
            raise ConfigError(f"el parámetro '{name}' no tiene ningún valor")
        normalised[name] = list(values)

    run_id = data.get("run_id", "{index:04d}")
    if not isinstance(run_id, str):
        raise ConfigError("'run_id' debe ser una cadena (plantilla)")

    results_root = data.get("results_root", "results")
    if not isinstance(results_root, str):
        raise ConfigError("'results_root' debe ser una cadena")

    unknown_top = set(data) - {
        "project", "command", "sweep", "run_id", "results_root", "slurm",
        "description", "version", "create_run_dirs", "cluster",
    }
    if unknown_top:
        raise ConfigError("claves desconocidas en la especificación: " + ", ".join(sorted(unknown_top)))

    create_run_dirs = data.get("create_run_dirs", True)
    if not isinstance(create_run_dirs, bool):
        raise ConfigError("'create_run_dirs' debe ser true o false")

    try:
        cluster = ClusterConfig.from_mapping(data.get("cluster"))
    except ClusterConfigError as exc:
        raise ConfigError(str(exc)) from None

    return SweepSpec(
        project=project,
        command=command,
        sweep=normalised,
        source=spec_path.resolve(),
        results_root=results_root,
        run_id=run_id,
        create_run_dirs=create_run_dirs,
        slurm=SlurmOptions.from_mapping(data.get("slurm")),
        cluster=cluster,
        raw=data,
    )
