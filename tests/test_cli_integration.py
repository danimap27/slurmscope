"""Prueba de integración: ciclo completo con el backend simulado.

Es la prueba que garantiza que las piezas encajan: plan → generar → enviar →
avanzar la simulación → sincronizar → resultados → informe.
"""

from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _helpers import write_spec  # noqa: E402
from slurmscope.cli import main  # noqa: E402
from slurmscope.config import load_spec  # noqa: E402
from slurmscope.ledger import Ledger  # noqa: E402


def run_cli(*argv) -> tuple[int, str]:
    """Ejecuta la CLI capturando stdout **y** stderr (los errores van a stderr)."""
    buffer = io.StringIO()
    code = 0
    with redirect_stdout(buffer), redirect_stderr(buffer):
        code = main(list(argv))
    return code, buffer.getvalue()


class TestCicloCompleto(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.cwd = Path.cwd()
        write_spec(self.dir)
        self.base = ["-C", str(self.dir), "--backend", "mock", "--mock-parallel", "2"]

    def tearDown(self):
        os.chdir(self.cwd)
        self.tmp.cleanup()

    def test_ciclo(self):
        code, out = run_cli(*self.base, "plan")
        self.assertEqual(code, 0)
        self.assertIn("ejecuciones   : 8", out)

        code, out = run_cli(*self.base, "gen")
        self.assertEqual(code, 0)
        self.assertTrue((self.dir / "plan" / "commands.txt").is_file())
        self.assertTrue((self.dir / "plan" / "submit_array.slurm").is_file())
        commands = (self.dir / "plan" / "commands.txt").read_text(encoding="utf-8").strip().splitlines()
        self.assertEqual(len(commands), 8)

        # volver a generar sin --force debe negarse
        code, out = run_cli(*self.base, "gen")
        self.assertEqual(code, 2)
        self.assertIn("ya existe", out)

        code, out = run_cli(*self.base, "submit", "-y")
        self.assertEqual(code, 0)
        self.assertIn("job array 900001", out)

        code, out = run_cli(*self.base, "status")
        self.assertEqual(code, 0)
        self.assertIn("AVANCE", out)

        run_cli(*self.base, "mock", "--advance", "120")
        code, out = run_cli(*self.base, "status")
        self.assertEqual(code, 0)
        ledger = Ledger(self.dir / ".slurmscope" / "ledger.db")
        counts = ledger.counts("prueba")
        # tras 120 s simulados: las 8 tareas están registradas y al menos una ha arrancado
        self.assertEqual(sum(counts.values()), 8)
        self.assertGreaterEqual(counts.get("RUNNING", 0) + counts.get("COMPLETED", 0), 1)

        run_cli(*self.base, "mock", "--advance", "20000")
        counts = Ledger(self.dir / ".slurmscope" / "ledger.db").counts("prueba")
        self.assertEqual(counts.get("COMPLETED", 0), 8)

        code, out = run_cli(*self.base, "estimate")
        self.assertEqual(code, 0)
        self.assertIn("histórico", out)

        code, out = run_cli(*self.base, "report")
        self.assertEqual(code, 0)
        report = self.dir / "reports" / "prueba-informe.md"
        self.assertTrue(report.is_file())
        self.assertIn("SlurmScope", report.read_text(encoding="utf-8"))
        payload = json.loads((self.dir / "reports" / "prueba-estado.json").read_text(encoding="utf-8"))
        self.assertEqual(payload["total"], 8)
        self.assertEqual(payload["counts"].get("COMPLETED"), 8)

    def test_resultados_en_disco(self):
        target = self.dir / "results" / "mnist" / "q4_s1"
        target.mkdir(parents=True)
        (target / "metrics.json").write_text(json.dumps({"metrics": {"accuracy": 0.91}}), encoding="utf-8")
        code, out = run_cli(*self.base, "results")
        self.assertEqual(code, 0)
        self.assertIn("accuracy=0.91", out)
        self.assertIn("media", out)

    def test_cancelar(self):
        run_cli(*self.base, "submit", "-y")
        code, out = run_cli(*self.base, "cancel")
        self.assertEqual(code, 0)
        self.assertIn("cancelado", out)
        ledger = Ledger(self.dir / ".slurmscope" / "ledger.db")
        self.assertEqual(ledger.counts("prueba").get("CANCELLED", 0), 8)

    def test_init_crea_plantilla(self):
        otro = Path(self.tmp.name) / "nuevo"
        otro.mkdir()
        code, out = run_cli("-C", str(otro), "init")
        self.assertEqual(code, 0)
        self.assertTrue((otro / "sweep.yaml").is_file())
        self.assertTrue((otro / ".gitignore").is_file())
        spec = load_spec(otro / "sweep.yaml")
        self.assertEqual(spec.project, "ejemplo-sweep")

    def test_sin_especificacion(self):
        vacio = Path(self.tmp.name) / "vacio"
        vacio.mkdir()
        code, _ = run_cli("-C", str(vacio), "plan")
        self.assertEqual(code, 2)

    def test_menu_plano_construye_secciones(self):
        from slurmscope.report import build_dashboard
        from slurmscope.tui import build_sections, render_section

        spec = load_spec(self.dir / "sweep.yaml")
        ledger = Ledger(self.dir / ".slurmscope" / "ledger.db")
        dash = build_dashboard(spec, ledger, backend=None, sync=False)
        sections = build_sections(dash)
        self.assertEqual(len(sections), 9)
        for key, _label, _renderer in sections:
            self.assertTrue(render_section(dash, key))


if __name__ == "__main__":
    unittest.main()
