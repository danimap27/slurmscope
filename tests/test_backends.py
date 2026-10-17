"""Pruebas de los backends: parseo de SLURM real y simulador determinista."""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from slurmscope.backends import (  # noqa: E402
    RealSlurm,
    get_backend,
    parse_slurm_elapsed,
    split_array_job_id,
)
from slurmscope.ledger import COMPLETED, FAILED, PENDING, RUNNING  # noqa: E402

SQUEUE = (
    "5316479_1|qtl|RUNNING|01:20:15|standard|None\n"
    "5316479_2|qtl|PENDING|00:00:00|standard|Priority\n"
)
SACCT = (
    "5316479_1|qtl|COMPLETED|0:0|01:20:15|2026-10-16T10:00:00|2026-10-16T11:20:15\n"
    "5316479_1.batch|batch|COMPLETED|0:0|01:20:10|2026-10-16T10:00:00|2026-10-16T11:20:10\n"
    "5316479_2|qtl|FAILED|1:0|00:03:00|2026-10-16T11:20:00|2026-10-16T11:23:00\n"
)


class TestParsers(unittest.TestCase):
    def test_parse_elapsed(self):
        self.assertEqual(parse_slurm_elapsed("01:20:15"), 4815.0)
        self.assertEqual(parse_slurm_elapsed("1-00:00:00"), 86400.0)
        self.assertEqual(parse_slurm_elapsed("2-03:04:05"), 2 * 86400 + 3 * 3600 + 4 * 60 + 5)
        self.assertIsNone(parse_slurm_elapsed(""))
        self.assertIsNone(parse_slurm_elapsed("basura"))

    def test_split_array_job_id(self):
        self.assertEqual(split_array_job_id("12345_7"), ("12345", 7))
        self.assertEqual(split_array_job_id("12345"), ("12345", None))
        self.assertEqual(split_array_job_id("12345_7.batch"), ("12345", 7))


class TestRealSlurm(unittest.TestCase):
    def setUp(self):
        self.calls = []

        def runner(args):
            self.calls.append(list(args))
            if args[0] == "squeue":
                return SQUEUE
            if args[0] == "sacct":
                return SACCT
            if args[0] == "sbatch":
                return "5316479;cluster\n"
            return ""

        self.backend = RealSlurm(user="dmartin", runner=runner)

    def test_submit(self):
        job_id = self.backend.submit(Path("plan/submit.slurm"), 8, job_name="qtl")
        self.assertEqual(job_id, "5316479")
        self.assertEqual(self.calls[-1][0], "sbatch")

    def test_queue(self):
        entries = self.backend.queue()
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0].array_job_id, "5316479")
        self.assertEqual(entries[0].task_index, 1)
        self.assertEqual(entries[0].state, RUNNING)
        self.assertEqual(entries[1].state, PENDING)
        self.assertEqual(entries[1].detail, "Priority")

    def test_accounting_ignora_pasos(self):
        entries = self.backend.accounting(["5316479"])
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0].state, COMPLETED)
        self.assertEqual(entries[1].state, FAILED)
        self.assertEqual(entries[1].exit_code, 1)


class TestMockSlurm(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.state = Path(self.tmp.name) / "mock.json"

    def tearDown(self):
        self.tmp.cleanup()

    def _backend(self, **kwargs):
        kwargs.setdefault("parallel", 2)
        kwargs.setdefault("min_seconds", 60)
        kwargs.setdefault("max_seconds", 120)
        return get_backend("mock", mock_state=self.state, **kwargs)

    def test_ciclo_completo(self):
        backend = self._backend()
        job_id = backend.submit(Path("s.slurm"), 4)
        self.assertEqual(job_id, "900001")
        self.assertEqual(len(backend.queue()), 4)

        backend.advance(120)
        entries = backend.accounting([job_id])
        terminal = [e for e in entries if e.state in (COMPLETED, FAILED)]
        vivas = [e for e in entries if e.state in (PENDING, RUNNING)]
        # con paralelismo 2 y duraciones de 60-120 s, en 120 s solo caben 2 tareas
        self.assertEqual(len(terminal), 2)
        self.assertEqual(len(vivas), 2)
        self.assertTrue(all(e.seconds and e.seconds >= 60 for e in terminal))

        backend.advance(600)
        entries = backend.accounting([job_id])
        self.assertTrue(all(e.state == COMPLETED for e in entries))
        self.assertEqual(backend.pending_count(), 0)
        self.assertTrue(all(e.seconds and e.seconds >= 60 for e in entries))

    def test_nunca_mas_tareas_simultaneas_que_el_tope(self):
        backend = self._backend(parallel=2)
        job_id = backend.submit(Path("s.slurm"), 6)
        for _ in range(10):
            backend.advance(30)
            corriendo = [e for e in backend.accounting([job_id]) if e.state == RUNNING]
            self.assertLessEqual(len(corriendo), 2)

    def test_determinismo(self):
        first = self._backend()
        first.submit(Path("s.slurm"), 3)
        first.advance(30)
        durations_a = [e.seconds for e in first.accounting(["900001"])]

        Path(self.state).unlink()
        second = self._backend()
        second.submit(Path("s.slurm"), 3)
        second.advance(30)
        durations_b = [e.seconds for e in second.accounting(["900001"])]
        self.assertEqual(durations_a, durations_b)

    def test_cancelar(self):
        backend = self._backend()
        backend.submit(Path("s.slurm"), 2)
        backend.cancel("900001")
        states = [e.state for e in backend.accounting(["900001"])]
        self.assertTrue(all(s == "CANCELLED" for s in states))

    def test_estado_persistente_entre_instancias(self):
        backend = self._backend()
        backend.submit(Path("s.slurm"), 1)
        otro = self._backend()
        self.assertEqual(len(otro.queue()), 1)

    def test_backend_desconocido(self):
        with self.assertRaises(Exception):
            get_backend("pbs")


if __name__ == "__main__":
    unittest.main()
