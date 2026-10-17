"""Utilidades compartidas por la batería de pruebas."""

from __future__ import annotations

import json
import textwrap
from pathlib import Path
from typing import Any, Dict, Optional


def write_spec(directory: Path, body: Optional[str] = None) -> Path:
    """Escribe una especificación de barrido mínima y devuelve su ruta."""
    content = body or textwrap.dedent(
        """\
        project: prueba
        run_id: "{dataset}/q{qubits}_s{seed}"
        command: "python runner.py --dataset {dataset} --qubits {qubits} --seed {seed} --out {run_dir}"
        results_root: results
        sweep:
          dataset: [mnist, fashion]
          qubits: [4, 6]
          seed: [1, 2]
        slurm:
          partition: standard
          time: "01:00:00"
          cpus_per_task: 2
          mem: 4G
          max_parallel: 4
        """
    )
    path = directory / "sweep.yaml"
    path.write_text(content, encoding="utf-8")
    return path


def write_result(directory: Path, run_dir: str, metrics: Dict[str, Any], marker: bool = True) -> Path:
    """Crea un directorio de resultados con métricas, como haría un runner real."""
    target = directory / run_dir
    target.mkdir(parents=True, exist_ok=True)
    (target / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    if marker:
        (target / ".done").write_text("ok\n", encoding="utf-8")
    return target
