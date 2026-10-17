"""Pruebas de los renderizadores: robustez de las tablas y de los informes."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _helpers import write_spec  # noqa: E402
from slurmscope.config import load_spec  # noqa: E402
from slurmscope.ledger import Ledger  # noqa: E402
from slurmscope.report import (  # noqa: E402
    build_dashboard,
    render_failures,
    render_overview,
    render_queue,
    render_results,
    render_runs,
    render_submissions,
    text_table,
    write_json,
    write_markdown,
)


class TestTextTable(unittest.TestCase):
    def test_tabla_basica(self):
        out = text_table(["a", "bb"], [["1", "22"], ["333", "4"]])
        lines = out.splitlines()
        self.assertEqual(len(lines), 4)  # cabecera + separador + 2 filas
        self.assertIn("a", lines[0])
        self.assertIn("333", lines[3])

    def test_fila_mas_larga_que_la_cabecera(self):
        out = text_table(["a", "b"], [["1", "2", "sobra"], ["3"]])
        lines = out.splitlines()
        self.assertEqual(len(lines), 4)
        self.assertNotIn("sobra", out)

    def test_fila_mas_corta_que_la_cabecera(self):
        out = text_table(["a", "b", "c"], [["1"]])
        self.assertIn("c", out.splitlines()[0])

    def test_valores_none_y_numericos(self):
        out = text_table(["a", "b"], [[None, 3.5]])
        self.assertIn("3.5", out)

    def test_anchos_minimos(self):
        out = text_table(["una cabecera larga"], [["x"]], max_width=1)
        self.assertTrue(out)

    def test_sin_columnas(self):
        self.assertEqual(text_table([], [["1"]]), "")

    def test_texto_muy_largo_se_recorta(self):
        out = text_table(["col"], [["x" * 100]], max_width=10)
        self.assertIn("…", out)
        self.assertLess(len(out.splitlines()[2]), 15)


class TestDashboardRender(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.spec = load_spec(write_spec(self.dir))
        self.ledger = Ledger(self.dir / ".slurmscope" / "ledger.db")
        self.dash = build_dashboard(self.spec, self.ledger, backend=None, sync=False)

    def tearDown(self):
        self.tmp.cleanup()

    def test_todos_los_renderizadores_funcionan_vacios(self):
        for renderer in (
            render_overview, render_queue, render_runs, render_results,
            render_failures, render_submissions,
        ):
            self.assertTrue(isinstance(renderer(self.dash), str))

    def test_avance_no_pasa_de_100(self):
        class R:
            def __init__(self, i):
                self.index, self.key, self.run_dir, self.params = i, f"k{i}", f"r{i}", {}

        # 20 registros para 8 ejecuciones: el avance no puede superar el 100 %
        self.ledger.record_submission("prueba", "mock", 8, [R(i) for i in range(1, 9)])
        self.ledger.apply_states(
            [{"submission_id": 1, "task_index": i, "state": "COMPLETED", "seconds": 10.0}
             for i in range(1, 9)]
        )
        self.ledger.record_submission("prueba", "mock", 8, [R(i) for i in range(1, 9)])
        dash = build_dashboard(self.spec, self.ledger, backend=None, sync=False)
        self.assertLessEqual(dash.progress, 100.0)

    def test_informes_exportables(self):
        md = write_markdown(self.dash, self.dir / "r.md")
        js = write_json(self.dash, self.dir / "r.json")
        self.assertIn("# SlurmScope", md.read_text(encoding="utf-8"))
        payload = json.loads(js.read_text(encoding="utf-8"))
        self.assertEqual(payload["project"], "prueba")
        self.assertEqual(len(payload["runs"]), 8)

    def test_aviso_de_sincronizacion_visible(self):
        class BackendRoto:
            name = "mock"

            def queue(self):
                raise RuntimeError("clúster caído")

            def accounting(self, job_ids):
                raise RuntimeError("clúster caído")

        class R:
            def __init__(self, i):
                self.index, self.key, self.run_dir, self.params = i, f"k{i}", f"r{i}", {}

        self.ledger.record_submission("prueba", "mock", 8, [R(i) for i in range(1, 9)],
                                      array_job_id="1")
        dash = build_dashboard(self.spec, self.ledger, backend=BackendRoto(), sync=True)
        self.assertTrue(any("cola" in w for w in dash.warnings))
        self.assertIn("aviso", render_overview(dash))


if __name__ == "__main__":
    unittest.main()
