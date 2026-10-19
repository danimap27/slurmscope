"""Registro persistente de envíos y ejecuciones (SQLite, sin dependencias).

El libro de ejecuciones es lo que permite responder a las tres preguntas del
panel: qué se ha lanzado, qué ha terminado y cuánto tarda cada cosa. Guarda, por
tarea de array, su estado, su código de salida y su duración real.
"""

from __future__ import annotations

import json
import sqlite3
import statistics
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

SCHEMA = """
CREATE TABLE IF NOT EXISTS submissions (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    project       TEXT    NOT NULL,
    backend       TEXT    NOT NULL,
    array_job_id  TEXT,
    commands_file TEXT,
    n_runs        INTEGER NOT NULL,
    submitted_at  TEXT    NOT NULL,
    status        TEXT    NOT NULL DEFAULT 'submitted'
);
CREATE TABLE IF NOT EXISTS runs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    submission_id INTEGER NOT NULL REFERENCES submissions(id) ON DELETE CASCADE,
    task_index    INTEGER NOT NULL,
    run_key       TEXT    NOT NULL,
    run_dir       TEXT,
    params        TEXT    NOT NULL DEFAULT '{}',
    state         TEXT    NOT NULL DEFAULT 'PENDING',
    exit_code     INTEGER,
    slurm_job_id  TEXT,
    started_at    TEXT,
    ended_at      TEXT,
    seconds       REAL,
    updated_at    TEXT    NOT NULL,
    UNIQUE (submission_id, task_index)
);
CREATE INDEX IF NOT EXISTS idx_runs_state ON runs(state);
CREATE INDEX IF NOT EXISTS idx_runs_key ON runs(run_key);
CREATE INDEX IF NOT EXISTS idx_submissions_project ON submissions(project);
"""

# Columnas añadidas en 1.3.0: uso de recursos según sacct (se migran solas)
USAGE_COLUMNS = (
    ("raw_state", "TEXT"),
    ("timelimit_seconds", "REAL"),
    ("alloc_cpus", "INTEGER"),
    ("req_mem_mb", "REAL"),
    ("max_rss_mb", "REAL"),
    ("cpu_seconds", "REAL"),
)

PENDING = "PENDING"
RUNNING = "RUNNING"
COMPLETED = "COMPLETED"
FAILED = "FAILED"
CANCELLED = "CANCELLED"
TERMINAL = {COMPLETED, FAILED, CANCELLED}


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def normalise_state(raw: str) -> str:
    """Traduce los estados de SLURM al vocabulario interno."""
    text = (raw or "").strip().upper()
    if text.startswith("PENDING") or text in {"PD", "CONFIGURING"}:
        return PENDING
    if text.startswith("RUNNING") or text in {"R", "CG", "COMPLETING"}:
        return RUNNING
    if text.startswith("COMPLETED") or text == "CD":
        return COMPLETED
    if text.startswith("CANCELLED") or text == "CA":
        return CANCELLED
    if text in {"", "UNKNOWN"}:
        return PENDING
    if text.startswith("FAILED") or text in {
        "F", "NF", "OOM", "TIMEOUT", "TO", "NODE_FAIL", "BOOT_FAIL",
        "DEADLINE", "OUT_OF_MEMORY", "PREEMPTED",
    }:
        return FAILED
    return FAILED


@dataclass
class Submission:
    id: int
    project: str
    backend: str
    array_job_id: Optional[str]
    commands_file: Optional[str]
    n_runs: int
    submitted_at: str
    status: str

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Submission":
        return cls(**{k: row[k] for k in row.keys()})


@dataclass
class RunRecord:
    id: int
    submission_id: int
    task_index: int
    run_key: str
    run_dir: Optional[str]
    params: Dict[str, Any]
    state: str
    exit_code: Optional[int]
    slurm_job_id: Optional[str]
    started_at: Optional[str]
    ended_at: Optional[str]
    seconds: Optional[float]
    raw_state: Optional[str] = None
    timelimit_seconds: Optional[float] = None
    alloc_cpus: Optional[int] = None
    req_mem_mb: Optional[float] = None
    max_rss_mb: Optional[float] = None
    cpu_seconds: Optional[float] = None

    @property
    def cause(self) -> Optional[str]:
        """Causa del fallo legible a partir del estado original de SLURM."""
        if self.state not in ("FAILED", "CANCELLED"):
            return None
        raw = (self.raw_state or "").upper()
        if raw in ("TIMEOUT", "DEADLINE"):
            return "tiempo agotado"
        if raw in ("OUT_OF_MEMORY", "OOM"):
            return "memoria agotada"
        if raw in ("NODE_FAIL", "BOOT_FAIL"):
            return "fallo del nodo"
        if raw == "PREEMPTED":
            return "desalojado"
        if raw.startswith("CANCELLED") or self.state == "CANCELLED":
            return "cancelado"
        if self.exit_code:
            return f"código de salida {self.exit_code}"
        return "error de ejecución"

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "RunRecord":
        data = {k: row[k] for k in row.keys()}
        data["params"] = json.loads(data.get("params") or "{}")
        return cls(**{k: data[k] for k in cls.__dataclass_fields__ if k in data})


class Ledger:
    """Libro de ejecuciones sobre SQLite."""

    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as conn:
            conn.executescript(SCHEMA)
            existing = {row["name"] for row in conn.execute("PRAGMA table_info(runs)")}
            for column, kind in USAGE_COLUMNS:
                if column not in existing:  # libros creados por versiones anteriores
                    conn.execute(f"ALTER TABLE runs ADD COLUMN {column} {kind}")
            conn.commit()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    # ── escritura ────────────────────────────────────────────────────────────
    def record_submission(
        self,
        project: str,
        backend: str,
        n_runs: int,
        runs: Sequence[Any],
        array_job_id: Optional[str] = None,
        commands_file: Optional[str] = None,
        task_offset: int = 0,
    ) -> int:
        """Registra un envío y sus tareas en estado PENDING.

        ``task_offset`` es el desplazamiento del trozo cuando el barrido se envía en
        varios job arrays: la ejecución ``index`` es la tarea ``index - task_offset``
        de su array, que es el índice que devuelven ``squeue`` y ``sacct``.
        """
        with closing(self._connect()) as conn:
            cur = conn.execute(
                "INSERT INTO submissions (project, backend, array_job_id, commands_file,"
                " n_runs, submitted_at) VALUES (?,?,?,?,?,?)",
                (project, backend, array_job_id, commands_file, n_runs, now_iso()),
            )
            submission_id = int(cur.lastrowid)
            stamp = now_iso()
            rows = []
            for run in runs:
                params = getattr(run, "params", {}) or {}
                rows.append(
                    (
                        submission_id,
                        int(getattr(run, "index", 0)) - int(task_offset),
                        str(getattr(run, "key", "")),
                        getattr(run, "run_dir", None),
                        json.dumps(params, ensure_ascii=False, sort_keys=True),
                        PENDING,
                        stamp,
                    )
                )
            conn.executemany(
                "INSERT OR REPLACE INTO runs (submission_id, task_index, run_key, run_dir,"
                " params, state, updated_at) VALUES (?,?,?,?,?,?,?)",
                rows,
            )
            conn.commit()
        return submission_id

    def apply_states(self, states: Iterable[Dict[str, Any]]) -> int:
        """Actualiza las tareas a partir de estados leídos del clúster.

        Cada entrada admite: ``task_index`` (o ``array_index``), ``state``,
        ``exit_code``, ``seconds``, ``slurm_job_id``, ``submission_id``.
        """
        updated = 0
        stamp = now_iso()
        with closing(self._connect()) as conn:
            for item in states:
                index = item.get("task_index", item.get("array_index"))
                submission_id = item.get("submission_id")
                if index is None or submission_id is None:
                    continue
                state = normalise_state(item.get("state", ""))
                exit_code = item.get("exit_code")
                seconds = item.get("seconds")
                started = item.get("started_at")
                ended = item.get("ended_at")
                cur = conn.execute(
                    """
                    UPDATE runs SET
                        state       = ?,
                        exit_code   = COALESCE(?, exit_code),
                        seconds     = COALESCE(?, seconds),
                        slurm_job_id= COALESCE(?, slurm_job_id),
                        started_at  = COALESCE(?, started_at),
                        ended_at    = COALESCE(?, ended_at),
                        raw_state   = COALESCE(?, raw_state),
                        timelimit_seconds = COALESCE(?, timelimit_seconds),
                        alloc_cpus  = COALESCE(?, alloc_cpus),
                        req_mem_mb  = COALESCE(?, req_mem_mb),
                        max_rss_mb  = COALESCE(?, max_rss_mb),
                        cpu_seconds = COALESCE(?, cpu_seconds),
                        updated_at  = ?
                    WHERE submission_id = ? AND task_index = ?
                    """,
                    (
                        state, exit_code, seconds, item.get("slurm_job_id"),
                        started, ended,
                        *(item.get(column) for column, _ in USAGE_COLUMNS),
                        stamp, submission_id, int(index),
                    ),
                )
                updated += cur.rowcount
            conn.commit()
        return updated

    def set_state(self, submission_id: int, state: str) -> None:
        with closing(self._connect()) as conn:
            conn.execute(
                "UPDATE submissions SET status = ? WHERE id = ?", (state, submission_id)
            )
            conn.commit()

    # ── lectura ──────────────────────────────────────────────────────────────
    def submissions(self, project: Optional[str] = None) -> List[Submission]:
        query = "SELECT * FROM submissions"
        args: List[Any] = []
        if project:
            query += " WHERE project = ?"
            args.append(project)
        query += " ORDER BY id DESC"
        with closing(self._connect()) as conn:
            return [Submission.from_row(r) for r in conn.execute(query, args)]

    def runs(
        self,
        submission_id: Optional[int] = None,
        state: Optional[str] = None,
        project: Optional[str] = None,
    ) -> List[RunRecord]:
        query = (
            "SELECT r.* FROM runs r JOIN submissions s ON s.id = r.submission_id WHERE 1=1"
        )
        args: List[Any] = []
        if submission_id is not None:
            query += " AND r.submission_id = ?"
            args.append(submission_id)
        if state:
            query += " AND r.state = ?"
            args.append(state)
        if project:
            query += " AND s.project = ?"
            args.append(project)
        query += " ORDER BY r.submission_id DESC, r.task_index ASC"
        with closing(self._connect()) as conn:
            return [RunRecord.from_row(r) for r in conn.execute(query, args)]

    def counts(self, project: Optional[str] = None) -> Dict[str, int]:
        """Recuento por estado del registro más reciente de cada ejecución.

        Si una ejecución se ha enviado varias veces (p. ej. se relanzan las fallidas),
        cuenta una sola vez, con su último estado: así el avance no supera el total ni
        un fallo ya corregido sigue contando como fallo.
        """
        inner = (
            "SELECT MAX(r2.id) FROM runs r2 JOIN submissions s2 ON s2.id = r2.submission_id"
        )
        args: List[Any] = []
        if project:
            inner += " WHERE s2.project = ?"
            args.append(project)
        inner += " GROUP BY r2.run_key"
        query = (
            "SELECT r.state AS state, COUNT(*) AS n FROM runs r "
            f"WHERE r.id IN ({inner}) GROUP BY r.state"
        )
        with closing(self._connect()) as conn:
            return {row["state"]: row["n"] for row in conn.execute(query, args)}

    def durations(
        self, project: Optional[str] = None, only_completed: bool = True
    ) -> List[float]:
        """Duraciones reales (segundos) de las tareas, para estimar tiempos."""
        query = (
            "SELECT r.seconds AS seconds FROM runs r "
            "JOIN submissions s ON s.id = r.submission_id "
            "WHERE r.seconds IS NOT NULL"
        )
        args: List[Any] = []
        if project:
            query += " AND s.project = ?"
            args.append(project)
        if only_completed:
            query += " AND r.state = 'COMPLETED'"
        with closing(self._connect()) as conn:
            return [float(row["seconds"]) for row in conn.execute(query, args)]

    def duration_stats(self, project: Optional[str] = None) -> Dict[str, Optional[float]]:
        values = self.durations(project=project)
        if not values:
            return {"n": 0, "mean": None, "median": None, "p90": None, "max": None}
        ordered = sorted(values)
        p90_index = min(len(ordered) - 1, int(round(0.9 * (len(ordered) - 1))))
        return {
            "n": float(len(values)),
            "mean": statistics.fmean(values),
            "median": statistics.median(values),
            "p90": ordered[p90_index],
            "max": max(values),
        }

    def latest(self, project: Optional[str] = None) -> Dict[str, RunRecord]:
        """Último intento de cada ejecución, por clave."""
        out: Dict[str, RunRecord] = {}
        for record in self.runs(project=project):  # más reciente primero
            out.setdefault(record.run_key, record)
        return out

    def attempts(self, project: Optional[str] = None) -> Dict[str, int]:
        """Cuántas veces se ha enviado cada ejecución."""
        out: Dict[str, int] = {}
        for record in self.runs(project=project):
            out[record.run_key] = out.get(record.run_key, 0) + 1
        return out

    def failures(self, project: Optional[str] = None, limit: int = 20) -> List[RunRecord]:
        query = (
            "SELECT r.* FROM runs r JOIN submissions s ON s.id = r.submission_id "
            "WHERE r.state IN ('FAILED','CANCELLED')"
        )
        args: List[Any] = []
        if project:
            query += " AND s.project = ?"
            args.append(project)
        query += " ORDER BY r.updated_at DESC LIMIT ?"
        args.append(limit)
        with closing(self._connect()) as conn:
            return [RunRecord.from_row(r) for r in conn.execute(query, args)]

    def export_csv(self, path: Path, project: Optional[str] = None) -> Path:
        import csv

        rows = self.runs(project=project)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(
                ["submission_id", "task_index", "run_key", "run_dir", "state",
                 "exit_code", "seconds", "started_at", "ended_at", "slurm_job_id", "params"]
            )
            for r in rows:
                writer.writerow(
                    [r.submission_id, r.task_index, r.run_key, r.run_dir, r.state,
                     r.exit_code, r.seconds, r.started_at, r.ended_at, r.slurm_job_id,
                     json.dumps(r.params, ensure_ascii=False, sort_keys=True)]
                )
        return path
