# Memoria técnica — SlurmScope v1.0.0

> Documento descriptivo de la obra, para acompañar la solicitud de inscripción en el
> Registro de la Propiedad Intelectual (véase `registro-software.md`).

## 1. Identificación de la obra

| Campo | Valor |
|---|---|
| Título | SlurmScope |
| Subtítulo | Orquestador de barridos de experimentos en clústeres SLURM con panel de resultados, historial de ejecuciones y estimación de tiempos |
| Versión | 1.0.0 |
| Tipo de obra | Programa de ordenador (obra literaria expresada en lenguaje de programación) |
| Lenguaje | Python 3 (compatible con 3.9 y posteriores) |
| Fecha de creación | 2026 |
| Autor | Daniel Martín Pérez |
| Titularidad | Daniel Martín Pérez (obra original, sin obra derivada de terceros) |
| Licencia de distribución | GNU GPL v3.0 o posterior |
| Repositorio | `~/proyectos/active/slurmscope` |

## 2. Resumen

SlurmScope resuelve un problema recurrente de la computación científica: lanzar cientos o
miles de ejecuciones de un experimento sobre un clúster SLURM y saber después, sin
ambigüedad, qué se ha ejecutado, qué ha terminado, qué ha fallado, cuánto ha tardado y
cuánto queda pendiente.

El programa no ejecuta el experimento en sí: orquesta su ejecución. Se le describe el
barrido en un fichero de texto (qué parámetros y qué valores forman el producto
cartesiano, y cómo se construye el comando de cada combinación) y a partir de ahí:

1. expande el barrido en una lista ordenada y reproducible de ejecuciones;
2. genera un fichero con un comando por línea y un script de *job array* de SLURM con una
   tarea por comando, respetando partición, tiempo, CPU, memoria y tope de paralelismo;
3. envía el job array y registra el envío en un libro local;
4. reconcilia el libro con el estado real del clúster consultando la cola y la
   contabilidad;
5. inspecciona el árbol de resultados en disco y agrega las métricas que encuentre;
6. estima el tiempo de cómputo restante y el tiempo hasta la finalización, a partir de
   las duraciones reales ya observadas;
7. muestra todo lo anterior en un panel interactivo y lo exporta en Markdown, JSON y CSV.

## 3. Motivación y originalidad

El flujo habitual en un HPC es generar a mano un fichero de comandos, enviarlo con
`sbatch --array`, mirar `squeue` en una terminal y bajar a contar carpetas de resultados
para saber por dónde va. Ese procedimiento tiene tres defectos: no deja historial, no
distingue «terminado» de «terminado con éxito» y no permite estimar lo que falta.

Las alternativas existentes cubren otra parte del problema: los gestores de flujos de
trabajo (Snakemake, Nextflow, Luigi) están orientados a describir dependencias entre
tareas más que a barrer parámetros; los seguidores de experimentos (MLflow, Weights &
Biases, TensorBoard) se centran en métricas de entrenamiento y exigen instrumentar el
código; las utilidades de SLURM (`sbatch`, `squeue`, `sacct`) no conservan estado propio.

SlurmScope ocupa el hueco intermedio y aporta, frente a ese conjunto:

- **cero dependencias externas**: se ejecuta con la biblioteca estándar de Python, lo que
  importa en clústeres donde no se puede instalar software sin módulos del sistema;
- **simulador de clúster incluido**: el mismo ciclo (envío, cola, contabilidad,
  estimaciones) funciona contra un SLURM simulado y determinista, de modo que el software
  se puede probar y demostrar sin acceso a un HPC;
- **histórico propio y persistente**: libro SQLite con envíos, tareas, estados, códigos de
  salida y duraciones, que es lo que permite estimar en vez de adivinar;
- **detección automática de resultados**: convierte el árbol de directorios en un estado
  legible (`OK`, `PARCIAL`, `PENDIENTE`, `FALLO`) y en un agregado de métricas;
- **estimación de dos cifras distintas**: cómputo pendiente y tiempo de pared, con margen
  conservador opcional (percentil 90);
- **lector YAML propio**: interpreta el subconjunto de YAML necesario sin PyYAML.

## 4. Arquitectura

El programa se organiza en doce módulos con una única dirección de dependencias
(la interfaz de línea de comandos es la única capa que toca todas las demás):

### 4.1 Diagrama de flujo

```
                         sweep.yaml / sweep.json
                                   │
                                   ▼
        ┌──────────────────────────────────────────────────┐
        │ config.load_spec()                               │
        │  · valida campos obligatorios                    │
        │  · rechaza claves desconocidas y valores erróneos│
        └──────────────────────┬───────────────────────────┘
                               │ SweepSpec
                               ▼
        ┌──────────────────────────────────────────────────┐
        │ matrix.expand_runs()                             │
        │  · producto cartesiano ordenado                  │
        │  · clave (run_id) única y segura                 │
        │  · directorio de resultados por ejecución        │
        └──────────────────────┬───────────────────────────┘
                               │ [Run, Run, …]
              ┌────────────────┴─────────────────┐
              ▼                                  ▼
  ┌───────────────────────────┐      ┌──────────────────────────────┐
  │ matrix.write_command_file │      │ matrix.render_slurm_script    │
  │  plan/commands.txt        │      │  plan/submit_array.slurm      │
  │  (un comando por línea)   │      │  (#SBATCH + array 1-N%M)      │
  └─────────────┬─────────────┘      └───────────────┬──────────────┘
                └──────────────┬─────────────────────┘
                               ▼
                  ┌─────────────────────────────┐
                  │ backend.submit()            │
                  │  · SLURM real: sbatch       │
                  │  · simulado: estado local   │
                  └──────────────┬──────────────┘
                                 ▼
                  ┌─────────────────────────────┐
                  │ ledger.record_submission()  │
                  │  envío + N tareas PENDING   │
                  └──────────────┬──────────────┘
                                 │
        ┌────────────────────────┴────────────────────────┐
        ▼                                                 ▼
┌───────────────────────┐                    ┌──────────────────────────┐
│ sync.sync_states()    │                    │ discover.scan_results()  │
│  squeue + sacct       │                    │  metrics.json / .done    │
│  → estado del clúster │                    │  → OK / PARCIAL / FALLO  │
└──────────┬────────────┘                    └────────────┬─────────────┘
           │ (escritura en el libro)                       │ (solo lectura)
           └──────────────────┬───────────────────────────┘
                              ▼
                 ┌─────────────────────────────┐
                 │ report.build_dashboard()    │
                 │  · recuentos por estado     │
                 │  · estimate.estimate_…()    │
                 │    cómputo pendiente + pared│
                 │  · agregado de métricas     │
                 └──────────────┬──────────────┘
                                ▼
        ┌───────────────────────┴────────────────────────┐
        ▼                        ▼                       ▼
┌───────────────┐   ┌────────────────────┐   ┌──────────────────────┐
│ tui.run_menu  │   │ cli: status,       │   │ report.write_…       │
│ panel curses  │   │ queue, results,    │   │  Markdown · JSON     │
│ 7 secciones   │   │ estimate, sync     │   │  ledger.export_csv   │
└───────────────┘   └────────────────────┘   └──────────────────────┘
```

### 4.2 Módulos

| Módulo | Responsabilidad |
|---|---|
| `config.py` | Carga y valida la especificación; resuelve rutas; define las opciones de SLURM |
| `matrix.py` | Producto cartesiano, claves de ejecución, comandos y script de job array |
| `backends.py` | Interfaz de clúster: backend SLURM real y simulador determinista |
| `ledger.py` | Libro de ejecuciones sobre SQLite: envíos, tareas, estados, duraciones |
| `sync.py` | Reconciliación entre el libro y el estado real del clúster |
| `discover.py` | Inspección del árbol de resultados y agregado de métricas |
| `estimate.py` | Estimación de tiempos y formateo de duraciones |
| `report.py` | Panel, tablas de texto e informes exportables (Markdown, JSON, CSV) |
| `tui.py` | Menú interactivo (curses) con alternativa en texto plano |
| `miniyaml.py` | Lector del subconjunto de YAML usado por la especificación |
| `cli.py` | Interfaz de línea de comandos y orquestación de los casos de uso |

Decisiones de diseño destacables:

- **El estado del clúster nunca se escribe a ciegas.** La contabilidad manda cuando una
  tarea alcanza un estado terminal; la ausencia de dato no borra un dato ya sabido.
- **La sincronización es idempotente**: puede invocarse tantas veces como se quiera sin
  alterar el resultado.
- **Los fallos de lectura del clúster no derriban el panel**: se recogen como avisos
  visibles en lugar de propagarse como excepciones.
- **Las claves de ejecución se validan**: una plantilla que produzca rutas absolutas o con
  `..` se rechaza, para que un barrido no pueda escribir fuera del proyecto.
- **Robustez del panel**: las tablas toleran filas con más o menos columnas que la
  cabecera, y el tamaño de los directorios de resultados se calcula con un tope de
  ficheros para no bloquear el refresco.

## 5. Especificación de entrada

Fichero `sweep.yaml` (o `.json`), con los campos: `project`, `run_id`, `command`,
`results_root`, `create_run_dirs`, `sweep` y `slurm`. La validación rechaza campos
obligatorios ausentes, valores que no sean listas, claves desconocidas y plantillas que
referencien parámetros inexistentes, con mensajes que indican qué corregir.

## 6. Interfaz de usuario

Línea de comandos con trece subcomandos: `init`, `plan`, `gen`, `submit`, `status`,
`menu`, `queue`, `results`, `estimate`, `report`, `sync`, `cancel`, `mock`, más `version`.
El panel (`menu`) ofrece siete secciones navegables —resumen, cola del clúster,
ejecuciones, resultados, estimación, envíos y fallos— con desplazamiento y refresco, y
dos acciones con confirmación: enviar un barrido y cancelar el último envío. Si no hay
terminal o curses no está disponible, degrada a un menú de texto equivalente.

## 7. Requisitos y entorno

- Python 3.9 o posterior. Ninguna dependencia obligatoria.
- Opcional: PyYAML (para YAML completo; sin él se usa el lector incluido) y pytest (la
  batería de pruebas usa `unittest`, de la biblioteca estándar).
- Para el backend real: `sbatch`, `squeue`, `sacct` y `scancel` accesibles en el PATH.
- Verificado en Linux, sobre clúster SLURM (CICA Hércules: partición `standard`) y en
  local con el backend simulado.

### Justificación de la ausencia de ejecutable

La obra es un programa interpretado escrito en Python y se distribuye como paquete de
código fuente que se ejecuta con el intérprete del sistema (`python3 -m slurmscope.cli`) o
se instala con `pip install .`, que genera el lanzador `slurmscope`. No existe, por tanto,
un binario compilado específico de la plataforma que pueda depositarse, y así se hace
constar a los efectos del apartado 2 de los requisitos específicos para programas de
ordenador. Si el registro requiere un artefacto ejecutable, puede generarse uno
autocontenido con la biblioteca estándar:

```
python3 -m zipapp slurmscope -o slurmscope.pyz -p "/usr/bin/env python3"
```

## 8. Verificación

La obra incluye una batería de 80 casos de prueba ejecutables sin dependencias
(`make test`), que cubren: el lector YAML; la validación de la especificación y sus
errores; la expansión del barrido y la detección de claves repetidas o inseguras; la
generación del script de job array; el libro de ejecuciones y la normalización de
estados; el parseo de la salida de `squeue` y `sacct`; el simulador (determinismo, tope de
paralelismo, cancelación, persistencia); la inspección de resultados y el agregado de
métricas; las estimaciones con y sin histórico; la robustez de las tablas del panel; y un
ciclo completo de integración a través de la interfaz de línea de comandos.

Además, `examples/demo.sh` reproduce el ciclo completo de principio a fin sobre el
clúster simulado.

## 9. Dimensiones de la obra

Cifras obtenidas con `python3 scripts/audit.py --json` (líneas de código = líneas con
contenido que no son comentario):

| Métrica | Valor |
|---|---|
| Ficheros de código del paquete | 12 |
| Líneas de código del paquete | 2 577 (3 045 líneas totales) |
| Funciones y métodos | 145 |
| Clases | 20 |
| Ficheros de la batería de pruebas | 9 |
| Líneas de código de las pruebas | 861 (1 061 líneas totales) |
| Casos de prueba | 80 |
| Ejemplo reproducible | 1 fichero, 46 líneas de código |

## 10. Licencia y cesión

La obra se distribuye bajo licencia GNU General Public License versión 3 o posterior
(texto íntegro en `LICENSE`). El autor conserva la titularidad de los derechos morales y
cede el uso en los términos de dicha licencia.

## 11. Anexo: árbol del código fuente

```
slurmscope/
  __init__.py                 metadatos y exportaciones públicas
  config.py                   especificación y validación
  matrix.py                   barrido, comandos y job array
  backends.py                 clúster real y simulador
  ledger.py                   libro de ejecuciones
  sync.py                     reconciliación libro ↔ clúster
  discover.py                 inspección de resultados
  estimate.py                 estimaciones de tiempo
  report.py                   panel e informes
  tui.py                      menú interactivo
  miniyaml.py                 lector YAML
  cli.py                      interfaz de línea de comandos
tests/                        80 casos de prueba (9 ficheros)
examples/                     demo reproducible y runner de ejemplo
scripts/
  audit.py                    auditoría y métricas de la obra
  preparar-registro.sh        generador del paquete de depósito
docs/                         esta memoria y la guía de registro
```

El listado de ficheros con líneas y huella SHA-256 de cada uno se genera con
`bash scripts/preparar-registro.sh` en `registro/listado-ficheros.txt`, y forma parte del
paquete de depósito junto con la totalidad del código fuente en un único fichero y un
`MANIFEST.sha256` con la integridad del conjunto.
