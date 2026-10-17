"""Reconciliación entre el libro local de ejecuciones y el estado del clúster.

Esta es la pieza que mantiene el historial fiel: lee la cola (``squeue``) y la
contabilidad (``sacct``) y vuelca estados, duraciones y códigos de salida en el
libro. Es idempotente: se puede llamar tantas veces como se quiera.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

from .backends import BaseBackend, QueueEntry
from .ledger import COMPLETED, FAILED, PENDING, RUNNING, Ledger


def _index_submissions(ledger: Ledger, project: Optional[str] = None) -> Dict[str, int]:
    """``array_job_id → submission_id`` para los envíos conocidos."""
    mapping: Dict[str, int] = {}
    for submission in ledger.submissions(project=project):
        if submission.array_job_id:
            mapping[str(submission.array_job_id)] = submission.id
    return mapping


def sync_states(
    ledger: Ledger,
    backend: BaseBackend,
    project: Optional[str] = None,
    job_ids: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """Vuelca el estado del clúster en el libro.

    Devuelve un resumen con cuántas tareas se han actualizado, cuántas entradas se
    han visto en cola y en contabilidad, y los avisos de lo que no se pudo leer (el
    clúster puede estar caído o el usuario no tener permiso: eso no debe tumbar el
    panel, pero tampoco debe pasar en silencio).
    """
    index = _index_submissions(ledger, project=project)
    if not index:
        return {"updated": 0, "queued": 0, "accounted": 0, "warnings": []}

    targets = list(job_ids) if job_ids else list(index)
    warnings: List[str] = []

    queued: List[QueueEntry] = []
    try:
        queued = [e for e in backend.queue() if (e.array_job_id or "") in index]
    except Exception as exc:
        warnings.append(f"no se pudo leer la cola ({exc})")

    accounted: List[QueueEntry] = []
    try:
        accounted = [e for e in backend.accounting(targets) if (e.array_job_id or "") in index]
    except Exception as exc:
        warnings.append(f"no se pudo leer la contabilidad ({exc})")

    merged: Dict[Tuple[int, int], Dict[str, object]] = {}
    for entry in queued:
        if entry.task_index is None:
            continue
        submission_id = index.get(str(entry.array_job_id))
        if submission_id is None:
            continue
        merged[(submission_id, entry.task_index)] = {
            "submission_id": submission_id,
            "task_index": entry.task_index,
            "state": entry.state,
            "seconds": entry.seconds,
            "slurm_job_id": entry.job_id,
        }
    for entry in accounted:
        if entry.task_index is None:
            continue
        submission_id = index.get(str(entry.array_job_id))
        if submission_id is None:
            continue
        key = (submission_id, entry.task_index)
        row = merged.setdefault(
            key,
            {"submission_id": submission_id, "task_index": entry.task_index},
        )
        # la contabilidad manda cuando la tarea ha llegado a un estado terminal
        row["state"] = entry.state
        row["seconds"] = entry.seconds if entry.seconds is not None else row.get("seconds")
        row["exit_code"] = entry.exit_code
        row["slurm_job_id"] = entry.job_id

    updated = ledger.apply_states(merged.values())
    return {"updated": updated, "queued": len(queued), "accounted": len(accounted),
            "warnings": warnings}


def local_status(ledger: Ledger, project: Optional[str] = None) -> Dict[str, int]:
    """Recuento por estado sin tocar el clúster."""
    counts = ledger.counts(project=project)
    return {
        "pending": counts.get(PENDING, 0),
        "running": counts.get(RUNNING, 0),
        "completed": counts.get(COMPLETED, 0),
        "failed": counts.get(FAILED, 0),
        "cancelled": counts.get("CANCELLED", 0),
    }
