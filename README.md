# SlurmScope

**Orquestador de barridos de experimentos en clústeres SLURM** con panel de resultados,
historial de ejecuciones y estimación de tiempos.

Escribes una especificación de barrido en YAML, y SlurmScope se encarga de expandir el
producto cartesiano, generar el job array, enviarlo, seguir su estado y darte un panel
donde ver qué ha terminado, qué falta, cuánto tarda cada cosa y cuánto queda.

- **Sin dependencias.** Solo la biblioteca estándar de Python (3.9+). Funciona en
  clústeres donde no puedes instalar nada.
- **Probable sin clúster.** Incluye un simulador determinista de SLURM (`--backend mock`),
  así que el ciclo completo se puede ejecutar y probar en un portátil.
- **Con historial.** Cada envío y cada tarea quedan registrados en un libro SQLite local:
  estados, códigos de salida y duraciones reales.
- **Con estimaciones útiles.** Tiempo de cómputo pendiente y tiempo de pared, calculados
  a partir del histórico real (mediana o percentil 90) y del paralelismo del job array.

## Instalación

No hace falta instalar nada para usarlo:

```bash
python3 -m slurmscope.cli --help      # desde el directorio del proyecto
python3 -m pip install .              # opcional: instala el comando `slurmscope`
```

## Inicio rápido

```bash
mkdir mi-barrido && cd mi-barrido
slurmscope init                        # crea sweep.yaml y .gitignore
$EDITOR sweep.yaml                     # define el barrido y el comando de cada ejecución
slurmscope plan                        # cuántas ejecuciones salen y cuáles
slurmscope gen                         # escribe plan/commands.txt y plan/submit_array.slurm
slurmscope submit                      # envía el job array y lo registra
slurmscope menu                        # panel interactivo
```

Sin clúster, para probarlo todo:

```bash
slurmscope submit --backend mock -y
slurmscope mock --advance 600          # avanza el reloj simulado
slurmscope status                      # panel de estado
```

Demostración completa y reproducible: `bash examples/demo.sh` (crea un proyecto temporal,
lanza un barrido de 16 ejecuciones en el clúster simulado y exporta los informes).

## La especificación (`sweep.yaml`)

```yaml
project: qtcl-ablation

run_id: "{dataset}/q{qubits}_d{depth}_s{seed}"     # clave única y ruta de resultados

command: >-
  python runner.py --dataset {dataset} --qubits {qubits}
  --depth {depth} --seed {seed} --out {run_dir}

results_root: results
create_run_dirs: true        # el comando se prefija con `mkdir -p <run_dir>`

sweep:                       # producto cartesiano (2×2×2×3 = 24 ejecuciones)
  dataset: [mnist, fashion]
  qubits: [4, 6]
  depth: [1, 3]
  seed: [1, 2, 3]

slurm:
  job_name: qtcl
  partition: standard
  time: "08:00:00"
  cpus_per_task: 4
  mem: 16G
  max_parallel: 40           # %N del job array: tope de tareas simultáneas
  log_dir: logs
  conda_base: /lustre/software/easybuild/common/software/Miniconda3/4.9.2
  conda_env: qtcl
  setup: ""                  # p. ej. "module load Python/3.11.3-GCCcore-12.3.0"
  extra_sbatch: []           # p. ej. ["#SBATCH --gres=gpu:1"]
```

En `command` puedes usar cualquier parámetro del barrido y además `{index}`, `{key}`,
`{run_dir}` y `{plan_out}`.

## Comandos

| Comando | Para qué |
|---|---|
| `init` | Crea una especificación de ejemplo y el `.gitignore` |
| `plan` | Muestra el plan de ejecuciones sin escribir nada |
| `gen` | Escribe `plan/commands.txt`, `plan/submit_array.slurm` y `plan/manifest.json` |
| `submit` | Genera y envía el job array, y lo registra en el libro |
| `status` | Resumen: avance, recuentos y estimación de tiempos |
| `menu` | Panel interactivo (curses; `--plain` si no hay terminal) |
| `queue` | Estado de la cola del clúster |
| `results` | Resultados encontrados en disco y agregado de métricas |
| `estimate` | Estimación detallada (`--p90` para ser conservador) |
| `report` | Exporta el informe en Markdown, JSON y CSV |
| `sync` | Vuelca el estado del clúster en el libro |
| `cancel` | Cancela un job array |
| `mock` | Avanza el clúster simulado (pruebas y demostraciones) |

Opciones globales: `-C/--directory`, `-s/--spec`, `--backend {slurm,mock}`,
`--mock-parallel`, `--fail-rate`.

## Cómo detecta los resultados

Para cada ejecución, SlurmScope busca en su directorio (`results_root` + `run_id`) un
fichero de métricas: `metrics.json`, `result.json`, `summary.json`, `runs.csv` o
`metrics.csv`. De un JSON lee el bloque `metrics` (y mezcla `params`/`config`/`run`) y de
un CSV, la última fila. También reconoce las marcas `.done`/`.complete`/`SUCCESS` y, en
sentido contrario, `FAILED`/`.failed`. Con eso clasifica cada ejecución en `OK`,
`PARCIAL`, `PENDIENTE` o `FALLO`, y agrega las métricas numéricas (n, media, desviación).

## Estados y estimaciones

Los estados de SLURM se normalizan a `PENDING`, `RUNNING`, `COMPLETED`, `FAILED` y
`CANCELLED`. La estimación usa la mediana de las duraciones reales de las tareas ya
completadas y, si todavía no hay histórico, el límite `--time` del script como cota
superior. Se muestran dos cifras distintas:

- **cómputo pendiente**: suma de lo que tardarán las tareas que faltan (lo que consumes);
- **tiempo de pared**: lo que tardarás en tenerlo todo, según `max_parallel`.

## Pruebas

```bash
make test          # o: PYTHONPATH=. python3 -m unittest discover -s tests -t tests
```

80 casos de prueba sobre la biblioteca estándar (`unittest`), sin dependencias: lector
YAML propio, validación de la especificación, expansión del barrido, generación del job
array, libro de ejecuciones, parseo de `squeue`/`sacct`, simulador, inspección de
resultados, estimaciones y un ciclo completo de integración por la CLI.

## Estructura

```
slurmscope/
  config.py      carga y valida la especificación
  matrix.py      producto cartesiano, comandos y script de job array
  backends.py    backend SLURM real y simulador determinista
  ledger.py      libro de ejecuciones (SQLite)
  sync.py        reconciliación libro ↔ clúster
  discover.py    inspección de resultados en disco
  estimate.py    estimaciones de tiempo
  report.py      panel, tablas e informes exportables
  tui.py         menú interactivo
  miniyaml.py    lector YAML sin dependencias
  cli.py         interfaz de línea de comandos
docs/            memoria técnica y guía de registro
examples/        demo reproducible y runner de ejemplo
tests/           80 casos de prueba
```

## Licencia

GPL-3.0-or-later. Autor: Daniel Martín Pérez.
