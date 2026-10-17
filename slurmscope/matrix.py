"""Expansión del barrido en ejecuciones concretas y generación de artefactos SLURM.

El producto cartesiano de ``spec.sweep`` se materializa en una lista ordenada y
estable de :class:`Run`. Cada ``Run`` tiene un índice (que es el
``SLURM_ARRAY_TASK_ID``), una clave legible (``run_id``) y su directorio de
resultados. A partir de esa lista se escriben el fichero de comandos y el script
de job array.
"""

from __future__ import annotations

import itertools
import re
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

from .config import ConfigError, SweepSpec

UNSAFE = re.compile(r"[^A-Za-z0-9._/@+-]+")


def slugify(value: Any) -> str:
    """Convierte un valor de parámetro en un fragmento de ruta seguro."""
    text = str(value).strip().replace(" ", "_")
    text = UNSAFE.sub("-", text)
    text = text.strip("-")
    return text or "x"


def _safe_key(key: str, position: int) -> str:
    """Valida que la clave de ejecución no pueda escaparse del directorio de resultados.

    Un ``run_id`` con ``..`` o con ruta absoluta escribiría fuera del proyecto: se
    rechaza en vez de confiar en que la plantilla esté bien.
    """
    parts = [p for p in key.split("/") if p not in ("", ".")]
    if not parts:
        raise ConfigError(
            f"la plantilla 'run_id' produjo una clave vacía en la posición {position}"
        )
    if key.startswith("/"):
        raise ConfigError(
            f"la plantilla 'run_id' produce la clave absoluta {key!r} en la posición "
            f"{position}: usa una ruta relativa al directorio de resultados"
        )
    if ".." in parts:
        raise ConfigError(
            f"la plantilla 'run_id' produce la clave insegura {key!r} en la posición "
            f"{position}: no puede contener '..'"
        )
    return "/".join(parts)


class _Missing(dict):
    """Ayuda a que ``str.format_map`` avise con claridad de una clave ausente."""

    def __missing__(self, key: str) -> str:  # pragma: no cover - se prueba en tests
        raise ConfigError(
            f"la plantilla usa '{{{key}}}' pero ese valor no existe; "
            f"disponibles: {', '.join(sorted(self))}"
        )


@dataclass(frozen=True)
class Run:
    """Una ejecución concreta del barrido."""

    index: int
    key: str
    params: Dict[str, Any]
    run_dir: str

    def as_dict(self) -> Dict[str, Any]:
        return {
            "index": self.index,
            "key": self.key,
            "run_dir": self.run_dir,
            "params": dict(self.params),
        }


def expand_runs(spec: SweepSpec) -> List[Run]:
    """Producto cartesiano de ``spec.sweep``, en orden estable y reproducible."""
    names = spec.params
    value_lists = [spec.sweep[name] for name in names]
    runs: List[Run] = []
    seen: Dict[str, int] = {}

    for position, combo in enumerate(itertools.product(*value_lists), start=1):
        params = dict(zip(names, combo))
        context = _Missing({name: slugify(value) for name, value in params.items()})
        context["index"] = position
        context["project"] = spec.project
        key = _safe_key(spec.run_id.format_map(context), position)
        if key in seen:
            raise ConfigError(
                f"la plantilla 'run_id' genera claves repetidas ('{key}' en las "
                f"posiciones {seen[key]} y {position}); añade un parámetro discriminante"
            )
        seen[key] = position

        run_dir = f"{spec.results_root.rstrip('/')}/{key}"
        runs.append(Run(index=position, key=key, params=params, run_dir=run_dir))

    return runs


def render_command(spec: SweepSpec, run: Run) -> str:
    """Sustituye los parámetros de ``run`` en la plantilla ``spec.command``.

    Si ``create_run_dirs`` está activo (por defecto), el comando se prefija con un
    ``mkdir -p`` del directorio de resultados de la ejecución: el runner no tiene
    que crearlo y una ejecución nunca se pierde por un directorio que falta.
    """
    context = _Missing({str(k): v for k, v in run.params.items()})
    context.update(
        {
            "index": run.index,
            "key": run.key,
            "run_dir": run.run_dir,
            "plan_out": spec.results_root,
            "project": spec.project,
        }
    )
    try:
        command = spec.command.format_map(context)
    except (KeyError, IndexError, ValueError) as exc:
        raise ConfigError(
            f"no pude construir el comando de la ejecución {run.key!r}: {exc}. "
            "Usa solo parámetros del barrido y {index}, {key}, {run_dir}, {plan_out}."
        ) from None

    if spec.create_run_dirs:
        return f"mkdir -p {shlex.quote(run.run_dir)} && {command}"
    return command


def _conda_block(spec: SweepSpec) -> List[str]:
    opts = spec.slurm
    if not opts.conda_env:
        return []
    lines = ['CONDA_BASE="${CONDA_BASE:-%s}"' % (opts.conda_base or "$HOME/miniconda3")]
    lines.append('if [ -f "$CONDA_BASE/etc/profile.d/conda.sh" ]; then')
    lines.append('    source "$CONDA_BASE/etc/profile.d/conda.sh"')
    lines.append("else")
    lines.append('    echo "[slurmscope] no encuentro conda.sh en $CONDA_BASE" >&2')
    lines.append("    exit 127")
    lines.append("fi")
    lines.append(f"conda activate {opts.conda_env}")
    return lines


def render_slurm_script(spec: SweepSpec, runs: Sequence[Run], commands_file: str) -> str:
    """Genera el script de job array que ejecuta un comando por tarea."""
    opts = spec.slurm
    n_runs = len(runs)
    array_expr = f"1-{n_runs}" + (f"%{opts.max_parallel}" if opts.max_parallel > 0 else "")

    header = [
        opts.shebang,
        f"#SBATCH --job-name={opts.job_name}",
        f"#SBATCH --output={opts.log_dir.rstrip('/')}/%x_%A_%a.out",
        f"#SBATCH --error={opts.log_dir.rstrip('/')}/%x_%A_%a.err",
        f"#SBATCH --partition={opts.partition}",
        f"#SBATCH --time={opts.time}",
        f"#SBATCH --cpus-per-task={opts.cpus_per_task}",
        f"#SBATCH --mem={opts.mem}",
        f"#SBATCH --array={array_expr}",
    ]
    header.extend(opts.extra_sbatch or [])

    body = [
        "",
        "# ─────────────────────────────────────────────────────────────",
        f"# Generado por SlurmScope para el barrido '{spec.project}'",
        f"# {n_runs} ejecuciones · máximo {opts.max_parallel or 'sin tope'} en paralelo",
        "# ─────────────────────────────────────────────────────────────",
        "",
        "set -u",
        'cd "${SLURM_SUBMIT_DIR:-$PWD}"',
        "",
    ]
    body.extend(_conda_block(spec))
    if spec.slurm.setup.strip():
        body.append(spec.slurm.setup.rstrip())
    body.extend(
        [
            "",
            f'COMMANDS_FILE="{commands_file}"',
            'if [ ! -f "$COMMANDS_FILE" ]; then',
            '    echo "[slurmscope] falta el fichero de comandos: $COMMANDS_FILE" >&2',
            "    exit 1",
            "fi",
            "",
            'CMD=$(sed -n "${SLURM_ARRAY_TASK_ID}p" "$COMMANDS_FILE")',
            'if [ -z "$CMD" ]; then',
            '    echo "[slurmscope] no hay comando para la tarea ${SLURM_ARRAY_TASK_ID}" >&2',
            "    exit 1",
            "fi",
            "",
            "# El fichero de comandos lo genera SlurmScope en la máquina de envío a partir",
            "# de la especificación del barrido, así que se ejecuta con eval para respetar",
            "# comillas y redirecciones. No lo edites a mano en el clúster.",
            'echo "task=${SLURM_ARRAY_TASK_ID} array=${SLURM_ARRAY_JOB_ID} inicio=$(date --iso-8601=seconds)"',
            'eval "$CMD"',
            'status=$?',
            'echo "task=${SLURM_ARRAY_TASK_ID} fin=$(date --iso-8601=seconds) status=${status}"',
            "exit ${status}",
            "",
        ]
    )
    return "\n".join(header + body)


def write_command_file(spec: SweepSpec, runs: Iterable[Run], path: Path) -> Path:
    """Escribe un fichero con un comando por línea (una tarea de array por línea)."""
    lines = [render_command(spec, run) for run in runs]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def plan_summary(spec: SweepSpec, runs: Sequence[Run], limit: int = 3) -> str:
    """Resumen legible del plan, para el menú y para la CLI."""
    out = [
        f"proyecto      : {spec.project}",
        f"ejecuciones   : {len(runs)}",
        f"parámetros    : {', '.join(f'{k}×{len(v)}' for k, v in spec.sweep.items())}",
        f"resultados en : {spec.results_path()}",
        f"partición     : {spec.slurm.partition} · {spec.slurm.time} · "
        f"{spec.slurm.cpus_per_task} CPU · {spec.slurm.mem}",
        f"paralelismo   : {spec.slurm.max_parallel or 'sin tope'}",
    ]
    for run in list(runs)[:limit]:
        out.append(f"  [{run.index}] {run.key} → {shlex.quote(render_command(spec, run))}")
    if len(runs) > limit:
        out.append(f"  … y {len(runs) - limit} más")
    return "\n".join(out)
