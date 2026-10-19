"""Eficiencia de los recursos pedidos y recomendaciones para el siguiente envío.

Todos los centros piden lo mismo: pedir el tiempo, la memoria y las CPU que se van a
usar (y comprobarlo con ``seff``). Un ``--time`` ajustado entra antes en cola; la
memoria o las CPU de más bloquean recursos y, en muchos centros, se cobran.

Este módulo hace el trabajo de ``seff`` para el barrido entero, a partir de la
contabilidad de SLURM que ya guarda el libro (``Elapsed``, ``Timelimit``,
``AllocCPUS``, ``ReqMem``, ``MaxRSS``, ``TotalCPU``), y propone valores concretos:

* ``--time``: percentil 95 de la duración de las tareas completadas con un margen
  del 25 %, redondeado a 15 min, sin superar el máximo de la QoS o partición.
  Si alguna tarea agotó el tiempo, se propone subirlo.
* ``--mem``: la mayor memoria usada con un margen del 20 % (y al menos 1 GB, como
  recomienda el CSUC), redondeada a GB. Si alguna se quedó sin memoria, el doble.
* ``--cpus-per-task``: si la eficiencia de CPU mediana es baja, las que de verdad
  se usan.
"""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

from .ledger import COMPLETED, RunRecord

TIME_MARGIN = 1.25
MEM_MARGIN = 1.20
MIN_MEM_EXTRA_MB = 1024.0
LOW_CPU_EFFICIENCY = 0.5


def _percentile(values: Sequence[float], q: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(math.ceil(q * len(ordered))) - 1)
    return ordered[max(0, index)]


def format_time(seconds: float) -> str:
    """Segundos → ``D-HH:MM:SS`` o ``HH:MM:SS`` (formato de ``--time``)."""
    seconds = int(round(seconds))
    days, rest = divmod(seconds, 86400)
    hours, rest = divmod(rest, 3600)
    minutes, secs = divmod(rest, 60)
    if days:
        return f"{days}-{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def format_mem(mb: float) -> str:
    gb = mb / 1024.0
    return f"{int(math.ceil(gb))}G" if gb >= 1 else f"{int(math.ceil(mb))}M"


@dataclass
class Suggestion:
    option: str
    current: Optional[str]
    proposed: str
    reason: str

    def as_dict(self) -> Dict[str, Any]:
        return {"option": self.option, "current": self.current,
                "proposed": self.proposed, "reason": self.reason}


@dataclass
class EfficiencyReport:
    n_completed: int = 0
    n_with_usage: int = 0
    time_usage_median: Optional[float] = None   # Elapsed / Timelimit
    time_usage_max: Optional[float] = None
    cpu_efficiency_median: Optional[float] = None
    mem_efficiency_median: Optional[float] = None
    mem_efficiency_max: Optional[float] = None
    max_rss_mb: Optional[float] = None
    p95_seconds: Optional[float] = None
    causes: Dict[str, int] = field(default_factory=dict)
    wasted_core_hours: Optional[float] = None
    suggestions: List[Suggestion] = field(default_factory=list)

    @property
    def has_data(self) -> bool:
        return self.n_completed > 0 or bool(self.causes)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "n_completed": self.n_completed,
            "n_with_usage": self.n_with_usage,
            "time_usage_median": self.time_usage_median,
            "time_usage_max": self.time_usage_max,
            "cpu_efficiency_median": self.cpu_efficiency_median,
            "mem_efficiency_median": self.mem_efficiency_median,
            "mem_efficiency_max": self.mem_efficiency_max,
            "max_rss_mb": self.max_rss_mb,
            "p95_seconds": self.p95_seconds,
            "causes": self.causes,
            "wasted_core_hours": self.wasted_core_hours,
            "suggestions": [s.as_dict() for s in self.suggestions],
        }


def analyse(records: Sequence[RunRecord], spec=None,
            max_wall_seconds: Optional[float] = None) -> EfficiencyReport:
    """Analiza el último intento de cada ejecución (``records``)."""
    report = EfficiencyReport()
    completed = [r for r in records if r.state == COMPLETED and r.seconds]
    report.n_completed = len(completed)

    for record in records:
        cause = record.cause
        if cause:
            report.causes[cause] = report.causes.get(cause, 0) + 1

    durations = [float(r.seconds) for r in completed]
    report.p95_seconds = _percentile(durations, 0.95)

    time_usage = [r.seconds / r.timelimit_seconds for r in completed
                  if r.timelimit_seconds]
    if time_usage:
        report.time_usage_median = round(statistics.median(time_usage), 4)
        report.time_usage_max = round(max(time_usage), 4)

    cpu_eff = [min(1.0, r.cpu_seconds / (r.seconds * r.alloc_cpus)) for r in completed
               if r.cpu_seconds is not None and r.alloc_cpus]
    if cpu_eff:
        report.cpu_efficiency_median = round(statistics.median(cpu_eff), 4)
        wasted = sum((1 - min(1.0, r.cpu_seconds / (r.seconds * r.alloc_cpus)))
                     * r.seconds * r.alloc_cpus for r in completed
                     if r.cpu_seconds is not None and r.alloc_cpus)
        report.wasted_core_hours = round(wasted / 3600.0, 2)

    rss = [float(r.max_rss_mb) for r in completed if r.max_rss_mb]
    mem_eff = [r.max_rss_mb / r.req_mem_mb for r in completed
               if r.max_rss_mb and r.req_mem_mb]
    if rss:
        report.max_rss_mb = max(rss)
    if mem_eff:
        report.mem_efficiency_median = round(statistics.median(mem_eff), 4)
        report.mem_efficiency_max = round(max(mem_eff), 4)
    report.n_with_usage = sum(1 for r in completed
                              if r.cpu_seconds is not None or r.max_rss_mb)

    if spec is not None:
        report.suggestions = _suggest(report, spec, records, max_wall_seconds)
    return report


def _suggest(report: EfficiencyReport, spec, records: Sequence[RunRecord],
             max_wall: Optional[float]) -> List[Suggestion]:
    from .backends import parse_slurm_memory
    from .rules import slurm_time_seconds

    out: List[Suggestion] = []
    opts = spec.slurm
    current_time = slurm_time_seconds(opts.time)
    timeouts = report.causes.get("tiempo agotado", 0)
    ooms = report.causes.get("memoria agotada", 0)

    # ── tiempo ───────────────────────────────────────────────────────────────
    proposed_time: Optional[float] = None
    reason = ""
    if timeouts and current_time:
        proposed_time = current_time * 1.5
        reason = f"{timeouts} tarea(s) agotaron el tiempo"
    elif report.p95_seconds and report.n_completed >= 3 and current_time:
        candidate = max(600.0, report.p95_seconds * TIME_MARGIN)
        if candidate < current_time * 0.6:
            proposed_time = candidate
            reason = (f"el 95 % terminó en {format_time(report.p95_seconds)} y se piden "
                      f"{opts.time}: un --time ajustado entra antes en cola")
    if proposed_time:
        proposed_time = math.ceil(proposed_time / 900.0) * 900.0
        if max_wall and proposed_time > max_wall:
            proposed_time = max_wall
            reason += " (limitado al máximo de la QoS/partición)"
        if not current_time or abs(proposed_time - current_time) >= 60:
            out.append(Suggestion("time", opts.time, format_time(proposed_time), reason))

    # ── memoria ──────────────────────────────────────────────────────────────
    current_mem = parse_slurm_memory(str(opts.mem or ""))
    if ooms and current_mem:
        out.append(Suggestion("mem", opts.mem, format_mem(current_mem * 2),
                              f"{ooms} tarea(s) se quedaron sin memoria"))
    elif report.max_rss_mb and current_mem and report.n_completed >= 3:
        candidate = max(report.max_rss_mb * MEM_MARGIN, report.max_rss_mb + MIN_MEM_EXTRA_MB)
        if candidate < current_mem * 0.6:
            out.append(Suggestion(
                "mem", opts.mem, format_mem(candidate),
                f"la que más usó necesitó {format_mem(report.max_rss_mb)} de {opts.mem}"))

    # ── CPU ──────────────────────────────────────────────────────────────────
    eff = report.cpu_efficiency_median
    if eff is not None and eff < LOW_CPU_EFFICIENCY and opts.cpus_per_task > 1 \
            and report.n_completed >= 3:
        cpus = max(1, int(math.ceil(opts.cpus_per_task * eff * 1.25)))
        if cpus < opts.cpus_per_task:
            out.append(Suggestion(
                "cpus_per_task", str(opts.cpus_per_task), str(cpus),
                f"eficiencia de CPU mediana del {eff:.0%}: el resto de núcleos espera "
                "sin hacer nada"))
    return out


def render_efficiency(report: EfficiencyReport) -> str:
    from .estimate import format_seconds
    from .report import text_table

    if not report.has_data:
        return ("Todavía no hay tareas terminadas con contabilidad. La eficiencia se "
                "calcula con sacct al sincronizar (slurmscope sync).")

    def pct(value: Optional[float]) -> str:
        return "—" if value is None else f"{value:.0%}"

    lines = [text_table(
        ["indicador", "valor"],
        [
            ["tareas completadas", report.n_completed],
            ["con datos de uso (sacct)", report.n_with_usage],
            ["duración p95", format_seconds(report.p95_seconds)],
            ["tiempo usado / pedido (mediana)", pct(report.time_usage_median)],
            ["tiempo usado / pedido (máximo)", pct(report.time_usage_max)],
            ["eficiencia de CPU (mediana)", pct(report.cpu_efficiency_median)],
            ["memoria usada / pedida (mediana)", pct(report.mem_efficiency_median)],
            ["memoria máxima usada",
             format_mem(report.max_rss_mb) if report.max_rss_mb else "—"],
            ["horas-núcleo ociosas",
             "—" if report.wasted_core_hours is None else f"{report.wasted_core_hours:g}"],
        ],
        ["l", "r"],
    )]
    if report.causes:
        lines += ["", "Fallos por causa:"]
        lines += [f"  {cause}: {n}" for cause, n in
                  sorted(report.causes.items(), key=lambda kv: -kv[1])]
    if report.suggestions:
        lines += ["", "Recomendado para el próximo envío (sweep.yaml, sección slurm):"]
        for s in report.suggestions:
            lines.append(f"  {s.option}: {s.proposed}    # ahora {s.current} · {s.reason}")
    elif report.n_completed >= 3:
        lines += ["", "Sin cambios recomendados para el próximo envío."]
    return "\n".join(lines)
