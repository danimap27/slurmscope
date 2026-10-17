"""Inspección de los resultados escritos en disco.

Es la mitad "de solo lectura" del panel: recorre el árbol de resultados, decide
qué ejecuciones han producido artefactos y extrae la primera fila de métricas
cuando existe un ``metrics.json``/``runs.csv``.
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

from .config import SweepSpec
from .matrix import Run

METRIC_FILES: Sequence[str] = (
    "metrics.json",
    "result.json",
    "summary.json",
    "runs.csv",
    "metrics.csv",
)
DONE_MARKERS: Sequence[str] = (".done", ".complete", "SUCCESS")

MAX_METRICS_KEYS = 12


@dataclass
class RunOutcome:
    """Estado de una ejecución visto desde el sistema de ficheros."""

    key: str
    run_dir: str
    exists: bool = False
    produced: bool = False
    failed: bool = False
    metrics: Dict[str, Any] = field(default_factory=dict)
    metrics_file: Optional[str] = None
    size_bytes: int = 0
    modified_at: Optional[float] = None

    @property
    def status(self) -> str:
        if self.failed:
            return "FALLO"
        if self.produced:
            return "OK"
        if self.exists:
            return "PARCIAL"
        return "PENDIENTE"


def read_metrics(path: Path) -> Dict[str, Any]:
    """Lee métricas de un JSON plano o de la última fila de un CSV."""
    try:
        if path.suffix.lower() == ".json":
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                if "metrics" in data and isinstance(data["metrics"], dict):
                    merged = dict(data.get("metrics") or {})
                    for key in ("params", "config", "run"):
                        if isinstance(data.get(key), dict):
                            merged.update(data[key])
                    return merged
                return data
            return {"value": data}
        with path.open(newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
            return dict(rows[-1]) if rows else {}
    except (OSError, ValueError, json.JSONDecodeError):
        return {}


def _dir_size(path: Path, max_files: int = 5000) -> int:
    """Tamaño del directorio de resultados, con tope de ficheros recorridos.

    El panel se refresca a menudo y un barrido grande puede dejar cientos de miles
    de ficheros: recorrerlos todos en cada refresco es inaceptable, así que se corta.
    """
    total = 0
    seen = 0
    try:
        for item in path.rglob("*"):
            if seen >= max_files:
                break
            try:
                if item.is_file():
                    total += item.stat().st_size
                    seen += 1
            except OSError:
                continue
    except OSError:
        pass  # permisos, enlaces roídos, NFS caído: el tamaño es informativo
    return total


def scan_run(spec: SweepSpec, run: Run, base: Optional[Path] = None) -> RunOutcome:
    """Inspecciona el directorio de resultados de una ejecución."""
    root = base or spec.source.parent
    run_path = root / run.run_dir
    outcome = RunOutcome(key=run.key, run_dir=str(run_path))

    if not run_path.is_dir():
        return outcome

    outcome.exists = True
    try:
        outcome.modified_at = run_path.stat().st_mtime
        outcome.size_bytes = _dir_size(run_path)
    except OSError:
        pass

    for marker in DONE_MARKERS:
        if (run_path / marker).exists():
            outcome.produced = True
    if (run_path / "FAILED").exists() or (run_path / ".failed").exists():
        outcome.failed = True

    for name in METRIC_FILES:
        candidate = run_path / name
        if candidate.is_file():
            outcome.metrics = read_metrics(candidate)
            outcome.metrics_file = name
            outcome.produced = outcome.produced or bool(outcome.metrics)
            break

    if not outcome.produced:
        try:
            outcome.produced = any(run_path.iterdir())
        except OSError:
            outcome.produced = False

    return outcome


def scan_results(
    spec: SweepSpec, runs: Iterable[Run], base: Optional[Path] = None
) -> Dict[str, RunOutcome]:
    """Inspecciona todas las ejecuciones y devuelve el mapa ``clave → resultado``."""
    return {run.key: scan_run(spec, run, base=base) for run in runs}


def aggregate_metrics(
    outcomes: Dict[str, RunOutcome], keys: Optional[Sequence[str]] = None
) -> Dict[str, Dict[str, float]]:
    """Media y desviación (poblacional) de las métricas numéricas presentes."""
    buckets: Dict[str, List[float]] = {}
    for outcome in outcomes.values():
        for key, value in outcome.metrics.items():
            if keys and key not in keys:
                continue
            try:
                number = float(value)
            except (TypeError, ValueError):
                continue
            buckets.setdefault(key, []).append(number)

    result: Dict[str, Dict[str, float]] = {}
    for key, values in buckets.items():
        if not values:
            continue
        mean = sum(values) / len(values)
        var = sum((v - mean) ** 2 for v in values) / len(values)
        result[key] = {
            "n": float(len(values)),
            "mean": mean,
            "std": var ** 0.5,
            "min": min(values),
            "max": max(values),
        }
    return result
