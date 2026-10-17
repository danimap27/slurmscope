# SlurmScope

**Orquestador de barridos de experimentos en clústeres SLURM** con panel de resultados,
historial de ejecuciones y estimación de tiempos. Funciona en Hércules (CICA), en los
nodos de la Red Española de Supercomputación y en cualquier clúster SLURM, en local o
por SSH desde tu máquina.

[![pruebas](https://github.com/danimap27/slurmscope/actions/workflows/tests.yml/badge.svg)](https://github.com/danimap27/slurmscope/actions/workflows/tests.yml)

Escribes una especificación de barrido en YAML, y SlurmScope se encarga de expandir el
producto cartesiano, generar el job array, enviarlo, seguir su estado y darte un panel
donde ver qué ha terminado, qué falta, cuánto tarda cada cosa y cuánto queda.

- **Sin dependencias.** Solo la biblioteca estándar de Python (3.9+). Funciona en
  clústeres donde no puedes instalar nada.
- **Probable sin clúster.** Incluye un simulador determinista de SLURM (`--backend mock`),
  así que el ciclo completo se puede ejecutar y probar en un portátil.
- **Con historial.** Cada envío y cada tarea quedan registrados en un libro SQLite local:
  estados, códigos de salida y duraciones reales.
- **Para cualquier centro.** Perfiles de Hércules, MareNostrum 5, FinisTerrae III,
  Tirant, Picasso y Caléndula; `slurmscope probe` lee del clúster lo que no se sabe.
- **Por SSH.** SlurmScope corre en tu máquina; en el clúster solo hacen falta bash y
  SLURM (vale aunque el Python del nodo de login sea antiguo).
- **Sin límites de array.** Los barridos que no caben en `MaxArraySize` se trocean
  solos en varios job arrays.
- **Con panel web.** `slurmscope serve` publica el estado de varios barridos en el
  navegador, en solo lectura.
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

## Clústeres: RES, Hércules y cualquier SLURM

Todos los nodos de la RES con guía pública y Hércules usan SLURM. Lo que cambia de un
centro a otro son sus normas, y cada perfil las recoge a partir de la **documentación
oficial del centro**, con la fuente de cada dato. Hay 20 perfiles: los 14 nodos de la
RES, Hércules, Albaicín, DIPC y uno genérico.

**[Normas y buenas prácticas de cada centro → docs/centros.md](docs/centros.md)**
(generado a partir de los perfiles con `python3 scripts/gen_centros.py`).

```bash
slurmscope clusters                    # todos los perfiles
slurmscope clusters --name finisterrae3  # QoS, límites, normas y fuentes
slurmscope check                       # tu barrido frente a las normas del centro
slurmscope acknowledge                 # texto de agradecimiento para publicaciones
```

Antes de enviar, `check` (y `submit`, que **no envía nada si hay errores**) comprueba:

- la cuenta, la QoS y la partición obligatorias;
- `--time` frente al máximo de la QoS o partición, incluida la asignación automática
  por tiempo de FinisTerrae III;
- 20 CPU por GPU en MareNostrum 5 ACC;
- que no haya descargas en centros sin internet;
- que no se ejecute desde `$HOME` donde está desaconsejado;
- el `MaxArraySize` real del clúster.

El script generado añade lo que exige cada centro:

- `--export=NONE` y `source /etc/profile.d/modules.sh` en Altamira;
- `--constraint=cal` en Picasso;
- `module purge` en LaPalma y TeideHPC;
- `OMP_NUM_THREADS` y `SRUN_CPUS_PER_TASK` iguales a las CPU pedidas, en todos.

**Oleadas por `MaxSubmit`.** En SLURM cada tarea de un array cuenta como un trabajo. En
FinisTerrae III con `class_b`, como mucho 15 en cola. `submit` envía solo lo que cabe y
`slurmscope resume` envía lo pendiente cuando avanza la cola; `resume --failed` relanza
solo lo fallido.

Con `cluster.allocation: res` se aplican la partición y la QoS RES de cada centro
(Caléndula `res`/`class_a`, Altamira `res`/`res_a`, Pirineus `res_cpu`, Agustina `RES`,
MareNostrum 5 `gp_resb`), salvo donde la QoS es automática.

Lo que la documentación no dice no se supone: `slurmscope probe` lo pregunta al clúster
en una sola sesión SSH (versión de SLURM, `MaxArraySize`, particiones, cuentas, QoS y sus
límites), y eso prevalece sobre el perfil.

```yaml
cluster:
  profile: hercules          # slurmscope clusters
  host: hercules-cica        # alias de ~/.ssh/config: activa el backend ssh
  remote_dir: slurmscope/{project}   # admite $LUSTRE/..., $FSCRATCH/... (se resuelven allí)
  # allocation: res          # horas de la RES: partición/QoS RES del centro
  # max_array_size: 500      # solo si quieres imponer otro valor
slurm:
  # account: mi_proyecto     # obligatorio en MareNostrum 5
  # qos: gp_resb
```

Con `host`, todas las órdenes de SLURM se ejecutan por SSH (`BatchMode=yes`: si la clave
no está autorizada falla en vez de pedir contraseña) y el proyecto se sube con `rsync`
antes de cada envío, sin borrar nada en el destino. Los resultados se traen con
`slurmscope pull` (`--logs` para los logs).

Antes de enviar se comprueban las reglas del centro (por ejemplo, la cuenta y la QoS en
MareNostrum 5): el error sale en tu terminal, no en la cola del clúster.

### Barridos más grandes que `MaxArraySize`

SLURM rechaza un array con índices iguales o mayores que `MaxArraySize`. SlurmScope parte
el barrido en trozos (en Hércules, 1200 ejecuciones = 499 + 499 + 202), envía el mismo
script con `--array=1-N` y `SLURMSCOPE_OFFSET`, y cada tarea ejecuta la línea
`SLURM_ARRAY_TASK_ID + offset`. Cada trozo es un envío distinto en el historial.

Verificado en Hércules el 17-10-2026: 6 ejecuciones en 2 job arrays (`5355721`,
`5355722`), 6/6 `COMPLETED`, y la tarea 1 del segundo array ejecutó la línea 4.

## Panel web

```bash
slurmscope serve --project exp-hercules --project exp-mn5 --port 8120
```

Una pestaña por barrido, con: avance por estados, estimación, historial de envíos,
ejecuciones filtrables (estado, búsqueda) con sus métricas, agregado de métricas, fallos,
cola del clúster y descarga en CSV. Es **de solo lectura**: no envía ni cancela nada.

La sincronización corre en un hilo aparte. Un clúster real se consulta como mucho cada 2
minutos, y nunca cuando no queda nada pendiente: el nodo de login es compartido.

Para demostraciones, un directorio con el fichero `.slurmscope-demo` usa siempre el
simulador, y con `--demo-tick S` avanza solo, genera métricas simuladas y se reinicia al
terminar. Sin ese fichero, el modo demo nunca escribe ni borra resultados.

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
| `check` | Contrasta el barrido con las normas del centro |
| `resume` | Envía lo pendiente por oleadas (`--failed` relanza lo fallido) |
| `acknowledge` | Agradecimiento que exige el centro en publicaciones |
| `clusters` | Perfiles de clúster incluidos (`--name` para el detalle) |
| `probe` | Lee del clúster sus límites, particiones, cuentas y QoS |
| `push` / `pull` | Sube el proyecto / descarga resultados del clúster remoto |
| `serve` | Panel web de uno o varios barridos |

Opciones globales: `-C/--directory`, `-s/--spec`, `--backend {auto,slurm,ssh,mock}`,
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

165 casos de prueba sobre la biblioteca estándar (`unittest`), sin dependencias: lector
YAML propio, validación de la especificación, expansión del barrido, generación del job
array, libro de ejecuciones, parseo de `squeue`/`sacct`, simulador, inspección de
resultados, estimaciones y un ciclo completo de integración por la CLI.

## Estructura

```
slurmscope/
  config.py      carga y valida la especificación
  matrix.py      producto cartesiano, comandos y script de job array
  clusters.py    configuración de clúster, troceo de arrays y sondeo
  clusters_data.py  normas de cada centro, con sus fuentes
  rules.py       comprobación previa al envío
  backends.py    SLURM local, SLURM por SSH y simulador determinista
  ledger.py      libro de ejecuciones (SQLite)
  sync.py        reconciliación libro ↔ clúster
  discover.py    inspección de resultados en disco
  estimate.py    estimaciones de tiempo
  report.py      panel, tablas e informes exportables
  tui.py         menú interactivo
  web.py         panel web de solo lectura
  miniyaml.py    lector YAML sin dependencias
  cli.py         interfaz de línea de comandos
docs/            memoria técnica y guía de registro
examples/        demo reproducible y runner de ejemplo
tests/           165 casos de prueba
```

## Licencia

GPL-3.0-or-later. Autor: Daniel Martín Pérez.
