"""Pruebas de la inspección de resultados y de la estimación de tiempos."""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _helpers import write_result, write_spec  # noqa: E402
from slurmscope.config import load_spec  # noqa: E402
from slurmscope.discover import aggregate_metrics, read_metrics, scan_results  # noqa: E402
from slurmscope.estimate import estimate_remaining, format_seconds, limit_seconds  # noqa: E402
from slurmscope.ledger import Ledger  # noqa: E402
from slurmscope.matrix import expand_runs  # noqa: E402


class TestDiscover(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.spec = load_spec(write_spec(self.dir))

    def tearDown(self):
        self.tmp.cleanup()

    def test_lee_json_con_bloque_metrics(self):
        path = self.dir / "m.json"
        path.write_text('{"params": {"a": 1}, "metrics": {"accuracy": 0.9}}', encoding="utf-8")
        self.assertEqual(read_metrics(path), {"accuracy": 0.9, "a": 1})

    def test_lee_csv_ultima_fila(self):
        path = self.dir / "runs.csv"
        path.write_text("epoch,loss\n1,0.5\n2,0.2\n", encoding="utf-8")
        self.assertEqual(read_metrics(path), {"epoch": "2", "loss": "0.2"})

    def test_fichero_corrupto_devuelve_vacio(self):
        path = self.dir / "m.json"
        path.write_text("{no es json", encoding="utf-8")
        self.assertEqual(read_metrics(path), {})

    def test_scan_detecta_ok_parcial_y_pendiente(self):
        write_result(self.dir, "results/mnist/q4_s1", {"accuracy": 0.9})
        (self.dir / "results/mnist/q4_s2").mkdir(parents=True)
        runs = expand_runs(self.spec)
        outcomes = scan_results(self.spec, runs)
        self.assertEqual(outcomes["mnist/q4_s1"].status, "OK")
        self.assertEqual(outcomes["mnist/q4_s2"].status, "PARCIAL")
        self.assertEqual(outcomes["fashion/q6_s2"].status, "PENDIENTE")
        self.assertTrue(outcomes["mnist/q4_s1"].produced)

    def test_marca_de_fallo(self):
        target = write_result(self.dir, "results/mnist/q4_s1", {"accuracy": 0.1})
        (target / "FAILED").write_text("", encoding="utf-8")
        outcomes = scan_results(self.spec, expand_runs(self.spec))
        self.assertEqual(outcomes["mnist/q4_s1"].status, "FALLO")

    def test_agregado(self):
        write_result(self.dir, "results/mnist/q4_s1", {"accuracy": 0.8})
        write_result(self.dir, "results/mnist/q4_s2", {"accuracy": 0.9})
        outcomes = scan_results(self.spec, expand_runs(self.spec))
        agg = aggregate_metrics(outcomes)
        self.assertEqual(agg["accuracy"]["n"], 2.0)
        self.assertAlmostEqual(agg["accuracy"]["mean"], 0.85, places=6)


class TestEstimate(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.spec = load_spec(write_spec(self.dir))
        self.ledger = Ledger(self.dir / "ledger.db")

    def tearDown(self):
        self.tmp.cleanup()

    def test_formato(self):
        self.assertEqual(format_seconds(45), "45 s")
        self.assertEqual(format_seconds(90), "1 min 30 s")
        self.assertEqual(format_seconds(3600), "1 h 0 min")
        self.assertEqual(format_seconds(90000), "1 d 1 h")
        self.assertEqual(format_seconds(None), "—")

    def test_limite_del_script(self):
        self.assertEqual(limit_seconds(self.spec), 3600.0)

    def test_sin_historico_usa_el_limite(self):
        runs = expand_runs(self.spec)
        est = estimate_remaining(self.ledger, len(runs), parallelism=4,
                                 fallback_seconds=limit_seconds(self.spec))
        self.assertEqual(est.n_pending, 8)
        self.assertEqual(est.per_run_seconds, 3600.0)
        self.assertIn("límite", est.source)
        self.assertEqual(est.wall_seconds, 2 * 3600.0)  # 8 pendientes / 4 en paralelo

    def test_con_historico_usa_la_mediana(self):
        class R:
            def __init__(self, i):
                self.index, self.key, self.run_dir, self.params = i, f"k{i}", f"r{i}", {}

        self.ledger.record_submission("prueba", "mock", 2, [R(1), R(2)])
        self.ledger.apply_states(
            [
                {"submission_id": 1, "task_index": 1, "state": "COMPLETED", "seconds": 600.0},
                {"submission_id": 1, "task_index": 2, "state": "FAILED", "seconds": 60.0},
            ]
        )
        est = estimate_remaining(self.ledger, 8, parallelism=4, fallback_seconds=3600,
                                 project="prueba")
        self.assertEqual(est.per_run_seconds, 600.0)
        self.assertEqual(est.n_completed, 1)
        self.assertEqual(est.n_failed, 1)
        self.assertEqual(est.n_pending, 6)
        self.assertIn("histórico", est.source)

    def test_todo_completado(self):
        class R:
            def __init__(self, i):
                self.index, self.key, self.run_dir, self.params = i, f"k{i}", f"r{i}", {}

        self.ledger.record_submission("prueba", "mock", 2, [R(1), R(2)])
        self.ledger.apply_states(
            [
                {"submission_id": 1, "task_index": 1, "state": "COMPLETED", "seconds": 100.0},
                {"submission_id": 1, "task_index": 2, "state": "COMPLETED", "seconds": 100.0},
            ]
        )
        est = estimate_remaining(self.ledger, 2, parallelism=2, project="prueba")
        self.assertEqual(est.n_pending, 0)
        self.assertEqual(est.wall_seconds, 0.0)


if __name__ == "__main__":
    unittest.main()
