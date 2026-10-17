"""Pruebas del lector YAML incluido (subconjunto sin dependencias)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from slurmscope.miniyaml import MiniYamlError, loads  # noqa: E402


class TestMiniYaml(unittest.TestCase):
    def test_escalares(self):
        data = loads(
            "entero: 4\nflotante: 1.5\ncierto: true\nfalso: false\nnulo: null\ntexto: hola\n"
        )
        self.assertEqual(data["entero"], 4)
        self.assertEqual(data["flotante"], 1.5)
        self.assertIs(data["cierto"], True)
        self.assertIs(data["falso"], False)
        self.assertIsNone(data["nulo"])
        self.assertEqual(data["texto"], "hola")

    def test_listas_en_linea(self):
        data = loads("sweep:\n  qubits: [4, 6, 8]\n  metodos: [ewc, replay]\n")
        self.assertEqual(data["sweep"]["qubits"], [4, 6, 8])
        self.assertEqual(data["sweep"]["metodos"], ["ewc", "replay"])

    def test_anidamiento_y_listas_en_bloque(self):
        data = loads(
            "slurm:\n"
            "  partition: standard\n"
            "  extra_sbatch:\n"
            "    - '#SBATCH --gres=gpu:1'\n"
            "    - '#SBATCH --exclusive'\n"
        )
        self.assertEqual(data["slurm"]["partition"], "standard")
        self.assertEqual(len(data["slurm"]["extra_sbatch"]), 2)
        self.assertEqual(data["slurm"]["extra_sbatch"][1], "#SBATCH --exclusive")

    def test_comentarios_y_lineas_vacias(self):
        data = loads("# cabecera\n\nproject: x   # en línea\n\nsweep:\n  a: [1]\n")
        self.assertEqual(data["project"], "x")
        self.assertEqual(data["sweep"]["a"], [1])

    def test_escalar_multilinea_doblado(self):
        data = loads(
            "command: >-\n"
            "  python runner.py --dataset {dataset}\n"
            "  --out {run_dir}\n"
            "sweep:\n"
            "  dataset: [mnist]\n"
        )
        self.assertEqual(data["command"], "python runner.py --dataset {dataset} --out {run_dir}")
        self.assertEqual(data["sweep"]["dataset"], ["mnist"])

    def test_escalar_multilinea_literal(self):
        data = loads("setup: |\n  module load A\n  module load B\notro: 1\n")
        self.assertEqual(data["setup"], "module load A\nmodule load B")
        self.assertEqual(data["otro"], 1)

    def test_comillas_con_dos_puntos_y_almohadilla(self):
        data = loads('hora: "08:00:00"\nnota: "a # b"\n')
        self.assertEqual(data["hora"], "08:00:00")
        self.assertEqual(data["nota"], "a # b")

    def test_mapa_en_linea(self):
        data = loads("limites: {cpu: 4, mem: 16G}\n")
        self.assertEqual(data["limites"], {"cpu": 4, "mem": "16G"})

    def test_error_de_sintaxis(self):
        with self.assertRaises(MiniYamlError):
            loads("clave sin dos puntos\n")

    def test_fichero_vacio(self):
        self.assertEqual(loads(""), {})


if __name__ == "__main__":
    unittest.main()
