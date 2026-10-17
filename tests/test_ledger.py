"""Pruebas del libro de ejecuciones (SQLite) y de la sincronización."""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from slurmscope.ledger import (  # noqa: E402
    COMPLETED,
    FAILED,
    PENDING,
    RUNNING,
    Ledger,
    normalise_state,
)


class FakeRun:
    def __init__(self, index, key, run_dir, params):
        self.index = index
        self.key = key
        self.run_dir = run_dir
        self.params = params


def sample_runs():
    return [
        FakeRun(1, "a/1", "results/a/1", {"a": 1}),
        FakeRun(2, "a/2", "results/a/2", {"a": 2}),
    ]


class TestLedger(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ledger = Ledger(Path(self.tmp.name) / "ledger.db")

    def tearDown(self):
        self.tmp.cleanup()

    def test_registrar_envio(self):
        submission_id = self.ledger.record_submission(
            "p", "mock", 2, sample_runs(), array_job_id="900001", commands_file="plan/commands.txt"
        )
        self.assertEqual(submission_id, 1)
        submissions = self.ledger.submissions("p")
        self.assertEqual(len(submissions), 1)
        self.assertEqual(submissions[0].array_job_id, "900001")
        runs = self.ledger.runs(submission_id=submission_id)
        self.assertEqual([r.state for r in runs], [PENDING, PENDING])
        self.assertEqual(runs[0].params, {"a": 1})

    def test_actualizar_estados(self):
        submission_id = self.ledger.record_submission("p", "mock", 2, sample_runs(), array_job_id="1")
        updated = self.ledger.apply_states(
            [
                {"submission_id": submission_id, "task_index": 1, "state": "COMPLETED",
                 "seconds": 120.0, "exit_code": 0},
                {"submission_id": submission_id, "task_index": 2, "state": "FAILED",
                 "seconds": 5.0, "exit_code": 1},
            ]
        )
        self.assertEqual(updated, 2)
        counts = self.ledger.counts("p")
        self.assertEqual(counts[COMPLETED], 1)
        self.assertEqual(counts[FAILED], 1)
        self.assertEqual(self.ledger.durations("p"), [120.0])

    def test_no_pisa_segundos_si_llega_vacio(self):
        submission_id = self.ledger.record_submission("p", "mock", 1, sample_runs()[:1])
        self.ledger.apply_states(
            [{"submission_id": submission_id, "task_index": 1, "state": "RUNNING", "seconds": 30.0}]
        )
        self.ledger.apply_states(
            [{"submission_id": submission_id, "task_index": 1, "state": "COMPLETED", "seconds": None}]
        )
        self.assertEqual(self.ledger.runs()[0].seconds, 30.0)

    def test_estadisticas_de_duracion(self):
        submission_id = self.ledger.record_submission("p", "mock", 3, sample_runs())
        self.ledger.apply_states(
            [
                {"submission_id": submission_id, "task_index": 1, "state": "COMPLETED", "seconds": 100.0},
                {"submission_id": submission_id, "task_index": 2, "state": "COMPLETED", "seconds": 300.0},
            ]
        )
        stats = self.ledger.duration_stats("p")
        self.assertEqual(stats["n"], 2.0)
        self.assertEqual(stats["median"], 200.0)
        self.assertEqual(stats["max"], 300.0)

    def test_fallos_y_exportacion(self):
        submission_id = self.ledger.record_submission("p", "mock", 2, sample_runs())
        self.ledger.apply_states(
            [{"submission_id": submission_id, "task_index": 2, "state": "FAILED", "exit_code": 137}]
        )
        failures = self.ledger.failures("p")
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0].exit_code, 137)

        csv_path = self.ledger.export_csv(Path(self.tmp.name) / "out.csv", project="p")
        text = csv_path.read_text(encoding="utf-8")
        self.assertIn("run_key", text)
        self.assertIn("a/2", text)

    def test_normalise_state(self):
        self.assertEqual(normalise_state("PD"), PENDING)
        self.assertEqual(normalise_state("RUNNING"), RUNNING)
        self.assertEqual(normalise_state("COMPLETED"), COMPLETED)
        self.assertEqual(normalise_state("OUT_OF_MEMORY"), FAILED)
        self.assertEqual(normalise_state("CANCELLED by 1000"), "CANCELLED")
        self.assertEqual(normalise_state(""), PENDING)


if __name__ == "__main__":
    unittest.main()
