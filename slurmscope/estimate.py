"""Estimación de tiempos a partir del histórico de ejecuciones.

Dos cantidades distintas, que el panel muestra por separado:

* **tiempo de cómputo pendiente**: suma de lo que se estima que tardarán las
  tareas que faltan (es lo que consume la cuota del clúster);
* **tiempo de pared**: cuánto se tardará en tenerlo todo, teniendo en cuenta
  cuántas tareas caben a la vez.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Optional

from .backends import parse_slurm_elapsed
from .ledger import Ledger


def format_seconds(seconds: Optional[float]) -> str:
    """Formatea una duración en algo legible (``3 h 12 min``)."""
    if seconds is None or seconds <= 0:
        return "—"
    seconds = float(seconds)
    if seconds < 60:
        return f"{seconds:.0f} s"
    minutes, sec = divmod(int(round(seconds)), 60)
    if minutes < 60:
        return f"{minutes} min" + (f" {sec} s" if sec and minutes < 10 else "")
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours} h {minutes} min"
    days, hours = divmod(hours, 24)
    return f"{days} d {hours} h"


@dataclass
class Estimate:
    """Resultado de la estimación de tiempos."""

    n_total: int
    n_completed: int
    n_failed: int
    n_pending: int
    per_run_seconds: Optional[float]
    source: str
    parallelism: int
    serial_seconds: Optional[float]
    wall_seconds: Optional[float]

    def as_dict(self) -> Dict[str, object]:
        return {
            "n_total": self.n_total,
            "n_completed": self.n_completed,
            "n_failed": self.n_failed,
            "n_pending": self.n_pending,
            "per_run_seconds": self.per_run_seconds,
            "source": self.source,
            "parallelism": self.parallelism,
            "serial_seconds": self.serial_seconds,
            "wall_seconds": self.wall_seconds,
            "per_run": format_seconds(self.per_run_seconds),
            "serial": format_seconds(self.serial_seconds),
            "wall": format_seconds(self.wall_seconds),
        }

    def eta(self, now: Optional[datetime] = None) -> Optional[str]:
        if not self.wall_seconds:
            return None
        stamp = (now or datetime.now()) + timedelta(seconds=self.wall_seconds)
        return stamp.isoformat(timespec="minutes")


def estimate_remaining(
    ledger: Ledger,
    n_total: int,
    parallelism: int,
    fallback_seconds: Optional[float] = None,
    project: Optional[str] = None,
    use_p90: bool = False,
) -> Estimate:
    """Estima el tiempo que falta para terminar el barrido.

    ``fallback_seconds`` se usa cuando no hay histórico todavía: normalmente el
    límite de tiempo solicitado en el script SLURM, que actúa como cota superior.
    """
    counts = ledger.counts(project=project)
    completed = counts.get("COMPLETED", 0)
    failed = counts.get("FAILED", 0) + counts.get("CANCELLED", 0)
    tracked = sum(counts.values())
    pending = max(n_total - completed - failed, 0)
    if tracked == 0:
        pending = n_total

    stats = ledger.duration_stats(project=project)
    per_run: Optional[float]
    if stats["n"] and stats["median"] is not None:
        per_run = float(stats["p90"] if use_p90 else stats["median"])
        source = f"histórico ({int(stats['n'])} tareas, {'p90' if use_p90 else 'mediana'})"
    elif fallback_seconds:
        per_run = float(fallback_seconds)
        source = "límite solicitado (sin histórico todavía)"
    else:
        per_run = None
        source = "sin datos"

    if per_run is None or pending == 0:
        return Estimate(
            n_total=n_total, n_completed=completed, n_failed=failed, n_pending=pending,
            per_run_seconds=per_run, source=source, parallelism=parallelism,
            serial_seconds=0.0 if pending == 0 else None,
            wall_seconds=0.0 if pending == 0 else None,
        )

    parallelism = max(int(parallelism or 1), 1)
    serial = per_run * pending
    wall = math.ceil(pending / parallelism) * per_run
    return Estimate(
        n_total=n_total, n_completed=completed, n_failed=failed, n_pending=pending,
        per_run_seconds=per_run, source=source, parallelism=parallelism,
        serial_seconds=serial, wall_seconds=wall,
    )


def limit_seconds(spec) -> Optional[float]:
    """Convierte el ``--time`` del script SLURM en segundos."""
    return parse_slurm_elapsed(_normalise(spec.slurm.time))


def _normalise(value: str) -> str:
    """Acepta ``08:00:00`` y ``8:00:00`` (SLURM admite ambas)."""
    text = (value or "").strip()
    if not text:
        return ""
    if "-" in text or ":" in text:
        parts = text.split(":")
        if len(parts) == 3 and len(parts[0]) == 1 and "-" not in parts[0]:
            parts[0] = "0" + parts[0]
            text = ":".join(parts)
    return text
