#!/usr/bin/env python3
"""Runner de ejemplo para la demostración de SlurmScope.

No entrena nada: escribe un ``metrics.json`` determinista a partir de los
argumentos, de modo que el panel tenga resultados reales que mostrar.
"""

from __future__ import annotations

import argparse
import json
import random
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", default="mnist")
    parser.add_argument("--qubits", type=int, default=4)
    parser.add_argument("--depth", type=int, default=1)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    rng = random.Random(f"{args.dataset}:{args.qubits}:{args.depth}:{args.seed}")
    base = 0.80 + 0.01 * args.qubits - 0.005 * args.depth
    accuracy = round(min(0.995, base + rng.uniform(-0.01, 0.01)), 4)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "metrics.json").write_text(
        json.dumps(
            {
                "params": {
                    "dataset": args.dataset,
                    "qubits": args.qubits,
                    "depth": args.depth,
                    "seed": args.seed,
                },
                "metrics": {
                    "accuracy": accuracy,
                    "f1": round(accuracy - 0.02, 4),
                    "seconds": round(rng.uniform(120, 900), 1),
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (out / ".done").write_text("ok\n", encoding="utf-8")
    print(f"[runner_demo] {args.out} accuracy={accuracy}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
