#!/usr/bin/env bash
# demo.sh — ciclo completo de SlurmScope sobre el clúster simulado.
# No necesita SLURM: sirve para probar el software y para la memoria técnica.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

export PYTHONPATH="$REPO"
SS="python3 -m slurmscope.cli"

cd "$WORK"
cp "$REPO/examples/runner_demo.py" runner.py

banner() { printf '\n\033[1m── %s ─────────────────────────────────────────────\033[0m\n' "$1"; }

banner "1. Crear el proyecto"
$SS init
sed -i 's/^  seed: \[1, 2, 3\]/  seed: [1, 2]/' sweep.yaml
sed -i 's/--dataset {dataset}/--dataset {dataset}/' sweep.yaml

banner "2. Plan (producto cartesiano)"
$SS plan

banner "3. Generar comandos + job array"
$SS gen --force
echo "--- plan/submit_array.slurm (cabecera) ---"
head -14 plan/submit_array.slurm

banner "4. Enviar al clúster simulado"
$SS submit --backend mock --mock-parallel 4 -y

banner "5. Avanzar la simulación y sincronizar"
$SS mock --backend mock --advance 900
$SS mock --backend mock --advance 900
$SS mock --backend mock --advance 900

banner "6. Panel de estado"
$SS status --backend mock

banner "7. Resultados en disco (simulados por el runner de ejemplo)"
python3 runner.py --dataset mnist --qubits 4 --depth 1 --seed 1 --out results/mnist/q4_d1_s1 >/dev/null
python3 runner.py --dataset mnist --qubits 4 --depth 1 --seed 2 --out results/mnist/q4_d1_s2 >/dev/null
python3 runner.py --dataset mnist --qubits 6 --depth 1 --seed 1 --out results/mnist/q6_d1_s1 >/dev/null
$SS results --backend mock

banner "8. Estimación de tiempos"
$SS estimate --backend mock

banner "9. Informes exportables"
$SS report --backend mock
ls -la reports/

banner "FIN"
echo "El proyecto de demostración vive en $WORK (se borra al salir)."
