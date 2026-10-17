"""Pruebas de expansión del barrido y generación de artefactos SLURM."""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _helpers import write_spec  # noqa: E402
from slurmscope.config import ConfigError, load_spec  # noqa: E402
from slurmscope.matrix import (  # noqa: E402
    expand_runs,
    plan_summary,
    render_command,
    render_slurm_script,
    slugify,
    write_command_file,
)


class TestMatrix(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.spec = load_spec(write_spec(self.dir))

    def tearDown(self):
        self.tmp.cleanup()

    def test_orden_estable(self):
        runs = expand_runs(self.spec)
        self.assertEqual(len(runs), 8)
        self.assertEqual(runs[0].index, 1)
        self.assertEqual(runs[0].key, "mnist/q4_s1")
        self.assertEqual(runs[0].params, {"dataset": "mnist", "qubits": 4, "seed": 1})
        self.assertEqual(runs[-1].key, "fashion/q6_s2")
        # el último parámetro varía más rápido
        self.assertEqual(expand_runs(self.spec), runs)

    def test_render_command(self):
        runs = expand_runs(self.spec)
        cmd = render_command(self.spec, runs[0])
        self.assertIn("--dataset mnist", cmd)
        self.assertIn("--out results/mnist/q4_s1", cmd)
        self.assertNotIn("{", cmd)

    def test_run_id_duplicado(self):
        body = (
            "project: x\n"
            'command: "echo {a}"\n'
            'run_id: "fijo"\n'
            "sweep:\n"
            "  a: [1, 2]\n"
        )
        spec = load_spec(write_spec(self.dir, body))
        with self.assertRaises(ConfigError) as ctx:
            expand_runs(spec)
        self.assertIn("repetidas", str(ctx.exception))

    def test_parametro_desconocido_en_command(self):
        body = 'project: x\ncommand: "echo {inexistente}"\nsweep:\n  a: [1]\n'
        spec = load_spec(write_spec(self.dir, body))
        with self.assertRaises(ConfigError) as ctx:
            render_command(spec, expand_runs(spec)[0])
        self.assertIn("inexistente", str(ctx.exception))

    def test_slugify(self):
        self.assertEqual(slugify("hello world"), "hello_world")
        self.assertEqual(slugify("a/b"), "a/b")
        self.assertEqual(slugify(""), "x")

    def test_script_slurm(self):
        runs = expand_runs(self.spec)
        script = render_slurm_script(self.spec, runs, "plan/commands.txt")
        self.assertIn("#SBATCH --partition=standard", script)
        self.assertIn("#SBATCH --array=1-8%4", script)
        self.assertIn("#SBATCH --time=01:00:00", script)
        self.assertIn("sed -n \"${SLURM_ARRAY_TASK_ID}p\"", script)
        self.assertIn("exit ${status}", script)

    def test_script_sin_tope_de_paralelismo(self):
        body = (
            "project: x\ncommand: \"echo {a}\"\nsweep:\n  a: [1, 2]\n"
            "slurm:\n  max_parallel: 0\n"
        )
        spec = load_spec(write_spec(self.dir, body))
        script = render_slurm_script(spec, expand_runs(spec), "c.txt")
        self.assertIn("#SBATCH --array=1-2\n", script)

    def test_fichero_de_comandos(self):
        runs = expand_runs(self.spec)
        path = write_command_file(self.spec, runs, self.dir / "plan" / "commands.txt")
        lines = path.read_text(encoding="utf-8").strip().splitlines()
        self.assertEqual(len(lines), 8)
        # por defecto se crea el directorio de resultados antes de ejecutar
        self.assertTrue(lines[0].startswith("mkdir -p results/mnist/q4_s1 && python runner.py"))
        self.assertEqual(len(set(lines)), 8)

    def test_sin_crear_directorios(self):
        body = (
            'project: x\ncommand: "echo {a}"\nsweep:\n  a: [1]\ncreate_run_dirs: false\n'
        )
        spec = load_spec(write_spec(self.dir, body))
        self.assertFalse(spec.create_run_dirs)
        self.assertEqual(render_command(spec, expand_runs(spec)[0]), "echo 1")

    def test_create_run_dirs_invalido(self):
        body = 'project: x\ncommand: "echo {a}"\nsweep:\n  a: [1]\ncreate_run_dirs: quizá\n'
        with self.assertRaises(ConfigError):
            load_spec(write_spec(self.dir, body))

    def test_run_id_con_punto_punto_es_rechazado(self):
        body = 'project: x\ncommand: "echo {a}"\nrun_id: "../{a}"\nsweep:\n  a: [1]\n'
        spec = load_spec(write_spec(self.dir, body))
        with self.assertRaises(ConfigError) as ctx:
            expand_runs(spec)
        self.assertIn("insegura", str(ctx.exception))

    def test_run_id_absoluto_es_rechazado(self):
        body = 'project: x\ncommand: "echo {a}"\nrun_id: "/tmp/{a}"\nsweep:\n  a: [1]\n'
        spec = load_spec(write_spec(self.dir, body))
        with self.assertRaises(ConfigError) as ctx:
            expand_runs(spec)
        self.assertIn("absoluta", str(ctx.exception))

    def test_run_id_normaliza_barras_repetidas(self):
        body = 'project: x\ncommand: "echo {a}"\nrun_id: "a//{a}"\nsweep:\n  a: [1]\n'
        spec = load_spec(write_spec(self.dir, body))
        self.assertEqual(expand_runs(spec)[0].key, "a/1")

    def test_resumen(self):
        text = plan_summary(self.spec, expand_runs(self.spec), limit=1)
        self.assertIn("ejecuciones   : 8", text)
        self.assertIn("dataset×2", text)


if __name__ == "__main__":
    unittest.main()
