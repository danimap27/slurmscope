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

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "RunRecord":
        data = {k: row[k] for k in row.keys()}
        data["params"] = json.loads(data.get("params") or "{}")
        return cls(**{k: data[k] for k in cls.__dataclass_fields__})


class Ledger:
    """Libro de ejecuciones sobre SQLite."""

    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as conn:
            conn.executescript(SCHEMA)
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
    ) -> int:
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
                        int(getattr(run, "index", 0)),
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
                        updated_at  = ?
                    WHERE submission_id = ? AND task_index = ?
                    """,
                    (
                        state, exit_code, seconds, item.get("slurm_job_id"),
                        started, ended, stamp, submission_id, int(index),
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
        query = (
            "SELECT r.state AS state, COUNT(*) AS n FROM runs r "
            "JOIN submissions s ON s.id = r.submission_id"
        )
        args: List[Any] = []
        if project:
            query += " WHERE s.project = ?"
            args.append(project)
        query += " GROUP BY r.state"
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
