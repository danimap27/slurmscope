"""Pruebas de carga y validación de la especificación."""

from __future__ import annotations

import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _helpers import write_spec  # noqa: E402
from slurmscope.config import ConfigError, SlurmOptions, load_spec  # noqa: E402


class TestConfig(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_carga_basica(self):
        path = write_spec(self.dir)
        spec = load_spec(path)
        self.assertEqual(spec.project, "prueba")
        self.assertEqual(spec.n_runs, 8)
        self.assertEqual(spec.params, ["dataset", "qubits", "seed"])
        self.assertEqual(spec.slurm.partition, "standard")
        self.assertEqual(spec.slurm.max_parallel, 4)
        self.assertTrue(str(spec.results_path()).endswith("results"))

    def test_deteccion_automatica(self):
        write_spec(self.dir)
        spec = load_spec(self.dir)
        self.assertEqual(spec.project, "prueba")

    def test_falta_project(self):
        path = write_spec(self.dir, "command: 'x'\nsweep:\n  a: [1]\n")
        with self.assertRaises(ConfigError) as ctx:
            load_spec(path)
        self.assertIn("project", str(ctx.exception))

    def test_falta_command(self):
        path = write_spec(self.dir, "project: x\nsweep:\n  a: [1]\n")
        with self.assertRaises(ConfigError) as ctx:
            load_spec(path)
        self.assertIn("command", str(ctx.exception))

    def test_sweep_vacio(self):
        path = write_spec(self.dir, "project: x\ncommand: 'y'\nsweep: {}\n")
        with self.assertRaises(ConfigError):
            load_spec(path)

    def test_sweep_valores_no_lista(self):
        path = write_spec(self.dir, "project: x\ncommand: 'y'\nsweep:\n  a: 3\n")
        with self.assertRaises(ConfigError) as ctx:
            load_spec(path)
        self.assertIn("lista", str(ctx.exception))

    def test_clave_desconocida(self):
        path = write_spec(self.dir, "project: x\ncommand: 'y'\nsweep:\n  a: [1]\nloquesea: 1\n")
        with self.assertRaises(ConfigError) as ctx:
            load_spec(path)
        self.assertIn("loquesea", str(ctx.exception))

    def test_opcion_slurm_desconocida(self):
        body = textwrap.dedent(
            """\
            project: x
            command: "y"
            sweep:
              a: [1]
            slurm:
              particion: standard
            """
        )
        path = write_spec(self.dir, body)
        with self.assertRaises(ConfigError) as ctx:
            load_spec(path)
        self.assertIn("particion", str(ctx.exception))

    def test_especificacion_json(self):
        path = self.dir / "sweep.json"
        path.write_text(
            '{"project": "j", "command": "echo {a}", "sweep": {"a": [1, 2]}}',
            encoding="utf-8",
        )
        spec = load_spec(path)
        self.assertEqual(spec.n_runs, 2)

    def test_extension_no_soportada(self):
        path = self.dir / "sweep.txt"
        path.write_text("project: x", encoding="utf-8")
        with self.assertRaises(ConfigError):
            load_spec(path)

    def test_opciones_slurm_por_defecto(self):
        opts = SlurmOptions.from_mapping(None)
        self.assertIsNone(opts.partition)  # la decide el perfil o el clúster
        self.assertEqual(opts.cpus_per_task, 4)
        self.assertEqual(opts.max_parallel, 0)


if __name__ == "__main__":
    unittest.main()
