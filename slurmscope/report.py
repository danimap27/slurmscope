"""Construcción del panel y de los informes exportables.

Aquí se reúne todo lo que el menú y los informes necesitan: recuentos por estado,
estimación de tiempos, resultados en disco, métricas agregadas y fallos recientes.
Los mismos datos alimentan el panel interactivo, el resumen de consola y los
ficheros de informe (CSV / Markdown).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

from .backends import BaseBackend
from .config import SweepSpec
from .discover import RunOutcome, aggregate_metrics, scan_results
from .estimate import Estimate, estimate_remaining, format_seconds, limit_seconds
from .ledger import Ledger, RunRecord
from .matrix import Run, expand_runs
from .sync import sync_states


def text_table(
    headers: Sequence[str],
    rows: Iterable[Sequence[Any]],
    aligns: Optional[Sequence[str]] = None,
    max_width: int = 42,
) -> str:
    """Tabla de texto con anchos calculados sobre el contenido.

    Tolera filas más largas o más cortas que la cabecera (se recortan o se rellenan):
    el panel no debe caerse porque un runner devuelva una columna de más.
    """
    headers = [str(h) for h in headers]
    n_cols = len(headers)
    if n_cols == 0:
        return ""

    prepared: List[List[str]] = []
    for row in rows:
        cells = [("" if cell is None else str(cell)) for cell in row][:n_cols]
        cells += [""] * (n_cols - len(cells))
        prepared.append(cells)

    widths = [len(h) for h in headers]
    for cells in prepared:
        for i, cell in enumerate(cells):
            widths[i] = max(widths[i], min(len(cell), max(1, max_width)))

    align_list = list(aligns or [])
    while len(align_list) < n_cols:
        align_list.append("l")

    def clip(text: str, width: int) -> str:
        if width < 1:
            return ""
        return text if len(text) <= width else text[: max(0, width - 1)] + "…"

    def line(cells: Sequence[str]) -> str:
        parts = []
        for i, cell in enumerate(cells):
            width = widths[i]
            clipped = clip(cell, width)
            parts.append(clipped.rjust(width) if align_list[i] == "r" else clipped.ljust(width))
        return "  ".join(parts).rstrip()

    out = [line(headers), line(["-" * w for w in widths])]
    out.extend(line(cells) for cells in prepared)
    return "\n".join(out)


@dataclass
class Dashboard:
    """Fotografía completa del estado del barrido."""

    spec: SweepSpec
    runs: List[Run]
    outcomes: Dict[str, RunOutcome]
    counts: Dict[str, int]
    estimate: Estimate
    submissions: List[Any]
    failures: List[RunRecord]
    metrics: Dict[str, Dict[str, float]]
    durations: Dict[str, float] = field(default_factory=dict)
    queue: List[Any] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    scan_error: Optional[str] = None
    generated_at: str = field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))

    # ── resumen numérico ─────────────────────────────────────────────────────
    @property
    def n_total(self) -> int:
        return len(self.runs)

    @property
    def n_on_disk(self) -> int:
        return sum(1 for o in self.outcomes.values() if o.produced and not o.failed)

    @property
    def n_failed_on_disk(self) -> int:
        return sum(1 for o in self.outcomes.values() if o.failed)

    @property
    def progress(self) -> float:
        if not self.n_total:
            return 0.0
        # Con envíos registrados manda el libro: los resultados en disco pueden ser de
        # un envío anterior y adelantarían el avance de uno nuevo. Sin libro (barrido
        # ejecutado fuera de SlurmScope), el disco es la única fuente.
        done = self.counts.get("COMPLETED", 0) if self.counts else self.n_on_disk
        return min(100.0, 100.0 * done / self.n_total)


def build_dashboard(
    spec: SweepSpec,
    ledger: Ledger,
    backend: Optional[BaseBackend] = None,
    sync: bool = True,
    base: Optional[Path] = None,
) -> Dashboard:
    """Reúne el estado del barrido en una sola estructura."""
    runs = expand_runs(spec)
    queue: List[Any] = []
    warnings: List[str] = []

    if sync and backend is not None:
        try:
            warnings.extend(sync_states(ledger, backend, project=spec.project).get("warnings", []))
        except Exception as exc:
            warnings.append(f"no se pudo sincronizar con el clúster ({exc})")
        try:
            queue = backend.queue()
        except Exception as exc:
            warnings.append(f"no se pudo leer la cola ({exc})")

    counts = ledger.counts(project=spec.project)
    submissions = ledger.submissions(project=spec.project)
    failures = ledger.failures(project=spec.project, limit=25)
    durations = {
        record.run_key: float(record.seconds)
        for record in ledger.runs(project=spec.project)
        if record.seconds is not None
    }

    scan_error: Optional[str] = None
    outcomes: Dict[str, RunOutcome] = {}
    try:
        outcomes = scan_results(spec, runs, base=base)
    except OSError as exc:  # pragma: no cover - defensivo
        scan_error = str(exc)

    root = base or spec.source.parent
    estimate = estimate_remaining(
        ledger,
        n_total=len(runs),
        parallelism=spec.slurm.max_parallel or len(runs) or 1,
        fallback_seconds=limit_seconds(spec),
        project=spec.project,
    )

    return Dashboard(
        spec=spec,
        runs=runs,
        outcomes=outcomes,
        counts=counts,
        estimate=estimate,
        submissions=submissions,
        failures=failures,
        metrics=aggregate_metrics(outcomes),
        durations=durations,
        queue=queue,
        warnings=warnings,
        scan_error=scan_error,
    )


# ─────────────────────────── renderizadores ──────────────────────────────────

def render_estimate_table(estimate: Estimate) -> str:
    """Tabla de la estimación de tiempos (la comparten la CLI y el panel)."""
    return text_table(
        ["concepto", "valor"],
        [
            ["ejecuciones totales", estimate.n_total],
            ["completadas", estimate.n_completed],
            ["pendientes", estimate.n_pending],
            ["fallidas", estimate.n_failed],
            ["duración por ejecución", format_seconds(estimate.per_run_seconds)],
            ["origen del dato", estimate.source],
            ["cómputo pendiente", format_seconds(estimate.serial_seconds)],
            ["tiempo de pared", format_seconds(estimate.wall_seconds)],
            ["tareas simultáneas", estimate.parallelism],
            ["finalización prevista", estimate.eta() or "—"],
        ],
        ["l", "r"],
    )


def render_overview(dash: Dashboard, width: int = 78) -> str:
    est = dash.estimate
    bar_len = max(20, min(60, width - 24))
    filled = int(round(bar_len * dash.progress / 100.0))
    bar = "█" * filled + "░" * (bar_len - filled)
    lines = [
        f"PROYECTO   {dash.spec.project}",
        f"AVANCE     [{bar}] {dash.progress:5.1f} %",
        "",
        text_table(
            ["métrica", "valor"],
            [
                ["ejecuciones totales", dash.n_total],
                ["completadas", dash.counts.get("COMPLETED", 0)],
                ["en cola o corriendo", dash.counts.get("RUNNING", 0) + dash.counts.get("PENDING", 0)],
                ["fallidas", dash.counts.get("FAILED", 0) + dash.counts.get("CANCELLED", 0)],
                ["resultados en disco", dash.n_on_disk],
                ["por ejecución (est.)", format_seconds(est.per_run_seconds)],
                ["origen de la estimación", est.source],
                ["cómputo pendiente (est.)", format_seconds(est.serial_seconds)],
                ["tiempo de pared (est.)", format_seconds(est.wall_seconds)],
                ["paralelismo", est.parallelism],
                ["finalización prevista", est.eta() or "—"],
            ],
            ["l", "r"],
        ),
    ]
    for warning in dash.warnings:
        lines.append(f"aviso: {warning}")
    if dash.scan_error:
        lines.append(f"aviso: no se pudo leer el árbol de resultados ({dash.scan_error})")
    return "\n".join(lines)


def render_queue(dash: Dashboard, limit: int = 25) -> str:
    if not dash.queue:
        return "No hay nada en cola (o el clúster no está accesible en este momento)."
    rows = []
    for entry in dash.queue[:limit]:
        rows.append(
            [
                entry.job_id,
                entry.name,
                entry.state,
                format_seconds(entry.seconds),
                entry.detail or "",
            ]
        )
    extra = ""
    if len(dash.queue) > limit:
        extra = f"\n… y {len(dash.queue) - limit} entradas más"
    return text_table(["job", "nombre", "estado", "transcurrido", "detalle"], rows, ["l", "l", "l", "r", "l"]) + extra


def render_runs(dash: Dashboard, state: Optional[str] = None, limit: int = 40) -> str:
    outcomes = dash.outcomes
    rows = []
    for run in dash.runs:
        outcome = outcomes.get(run.key)
        disk = outcome.status if outcome else "—"
        if state and disk != state:
            continue
        metrics = ""
        if outcome and outcome.metrics:
            first = list(outcome.metrics.items())[:2]
            metrics = ", ".join(f"{k}={_short(v)}" for k, v in first)
        rows.append([run.index, run.key, disk, format_seconds(_run_seconds(dash, run.key)), metrics])
        if len(rows) >= limit:
            break
    if not rows:
        return "Sin ejecuciones que mostrar con ese filtro."
    body = text_table(
        ["#", "ejecución", "disco", "duración", "métricas"], rows, ["r", "l", "l", "r", "l"]
    )
    if len(dash.runs) > limit:
        body += f"\n(mostrando {limit} de {len(dash.runs)})"
    return body


def _run_seconds(dash: Dashboard, key: str) -> Optional[float]:
    """Duración real registrada para una ejecución, si ya terminó."""
    return dash.durations.get(key)


def _short(value: Any) -> str:
    text = f"{value:.4g}" if isinstance(value, float) else str(value)
    return text if len(text) <= 12 else text[:11] + "…"


def render_results(dash: Dashboard, limit: int = 30) -> str:
    rows = []
    for run in dash.runs:
        outcome = dash.outcomes.get(run.key)
        if not outcome or not outcome.produced:
            continue
        rows.append(
            [
                run.key,
                outcome.status,
                outcome.metrics_file or "—",
                _human_size(outcome.size_bytes),
                ", ".join(f"{k}={_short(v)}" for k, v in list(outcome.metrics.items())[:3]),
            ]
        )
        if len(rows) >= limit:
            break
    if not rows:
        return "Todavía no hay resultados en disco."
    body = text_table(["ejecución", "estado", "métricas en", "tamaño", "valores"], rows, ["l", "l", "l", "r", "l"])
    if dash.metrics:
        body += "\n\nAgregado:\n"
        body += text_table(
            ["métrica", "n", "media", "desv."],
            [[k, int(v["n"]), f"{v['mean']:.4g}", f"{v['std']:.3g}"] for k, v in sorted(dash.metrics.items())],
            ["l", "r", "r", "r"],
        )
    return body


def render_failures(dash: Dashboard) -> str:
    if not dash.failures:
        return "Sin fallos registrados."
    rows = [
        [r.submission_id, r.task_index, r.run_key, r.exit_code, r.ended_at or ""]
        for r in dash.failures
    ]
    return text_table(["envío", "#", "ejecución", "código", "terminada"], rows, ["r", "r", "l", "r", "l"])


def render_submissions(dash: Dashboard) -> str:
    if not dash.submissions:
        return "Todavía no se ha enviado nada."
    rows = [
        [s.id, s.array_job_id or "—", s.n_runs, s.status, s.submitted_at, Path(s.commands_file or "").name]
        for s in dash.submissions
    ]
    return text_table(["envío", "job array", "tareas", "estado", "enviado", "comandos"], rows, ["r", "l", "r", "l", "l", "l"])


def _human_size(size: int) -> str:
    step = 1024.0
    value = float(size)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < step or unit == "TB":
            return f"{value:.0f} {unit}" if unit == "B" else f"{value:.1f} {unit}"
        value /= step
    return f"{value:.1f} TB"


# ─────────────────────────── exportación ─────────────────────────────────────

def write_markdown(dash: Dashboard, path: Path) -> Path:
    """Informe completo en Markdown (para pegar en el cuaderno o en el paper)."""
    blocks = [
        f"# SlurmScope · {dash.spec.project}",
        f"_Generado el {dash.generated_at}_",
        "",
        "## Resumen",
        "```",
        render_overview(dash),
        "```",
        "",
        "## Cola",
        "```",
        render_queue(dash),
        "```",
        "",
        "## Ejecuciones",
        "```",
        render_runs(dash, limit=200),
        "```",
        "",
        "## Resultados",
        "```",
        render_results(dash, limit=200),
        "```",
        "",
        "## Envíos",
        "```",
        render_submissions(dash),
        "```",
        "",
        "## Fallos",
        "```",
        render_failures(dash),
        "```",
        "",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(blocks), encoding="utf-8")
    return path


def dashboard_payload(dash: Dashboard, ledger: Optional[Ledger] = None) -> Dict[str, Any]:
    """Volcado estructurado del panel (lo usan el informe JSON y el panel web).

    Con ``ledger`` se añade, para cada ejecución, su estado más reciente en el libro
    (estado, duración, código de salida y job de SLURM).
    """
    spec = dash.spec
    latest: Dict[str, RunRecord] = {}
    if ledger is not None:
        for record in ledger.runs(project=spec.project):  # más reciente primero
            latest.setdefault(record.run_key, record)

    def run_entry(run: Run) -> Dict[str, Any]:
        outcome = dash.outcomes.get(run.key)
        record = latest.get(run.key)
        return {
            "index": run.index,
            "key": run.key,
            "params": run.params,
            "disk": outcome.status if outcome else None,
            "metrics": outcome.metrics if outcome else {},
            "state": record.state if record else None,
            "seconds": record.seconds if record else dash.durations.get(run.key),
            "exit_code": record.exit_code if record else None,
            "slurm_job_id": record.slurm_job_id if record else None,
        }

    return {
        "project": spec.project,
        "generated_at": dash.generated_at,
        "cluster": {
            "profile": spec.cluster.profile,
            "host": spec.cluster.host,
            "partition": spec.effective_partition,
            "account": spec.slurm.account,
            "qos": spec.slurm.qos,
            "max_array_size": spec.cluster.effective_max_array_size(),
        },
        "slurm": {
            "time": spec.slurm.time,
            "cpus_per_task": spec.slurm.cpus_per_task,
            "mem": spec.slurm.mem,
            "max_parallel": spec.slurm.max_parallel,
        },
        "sweep": {name: list(values) for name, values in spec.sweep.items()},
        "total": dash.n_total,
        "counts": dash.counts,
        "on_disk": dash.n_on_disk,
        "progress": dash.progress,
        "estimate": dict(dash.estimate.as_dict(), eta=dash.estimate.eta()),
        "metrics": dash.metrics,
        "warnings": list(dash.warnings) + ([dash.scan_error] if dash.scan_error else []),
        "queue": [
            dict(e.as_dict(), name=getattr(e, "name", ""), detail=getattr(e, "detail", ""))
            for e in dash.queue if hasattr(e, "as_dict")
        ],
        "failures": [
            {"submission_id": r.submission_id, "task_index": r.task_index,
             "key": r.run_key, "exit_code": r.exit_code, "ended_at": r.ended_at}
            for r in dash.failures
        ],
        "submissions": [
            {
                "id": s.id,
                "backend": s.backend,
                "array_job_id": s.array_job_id,
                "n_runs": s.n_runs,
                "status": s.status,
                "submitted_at": s.submitted_at,
            }
            for s in dash.submissions
        ],
        "runs": [run_entry(run) for run in dash.runs],
    }


def write_json(dash: Dashboard, path: Path, ledger: Optional[Ledger] = None) -> Path:
    """Volcado estructurado del panel, para consumirlo desde otros scripts."""
    payload = dashboard_payload(dash, ledger)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path
