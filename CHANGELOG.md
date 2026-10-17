# Historial de cambios

## 1.1.0 — 2026-10-17

- Perfiles de clúster (`cluster.profile`): Hércules, MareNostrum 5 GPP y ACC,
  FinisTerrae III, Tirant, Picasso, Caléndula y genérico. Solo datos comprobados.
- Backend `ssh`: SlurmScope corre en local y controla el clúster por SSH; subida del
  proyecto con `rsync` (sin borrar en destino) y `pull` de resultados y logs.
- Troceo automático de barridos que superan `MaxArraySize` (offset por trozo).
- `slurmscope probe`: versión de SLURM, `MaxArraySize`, particiones, cuentas y QoS.
- `slurm.account` y `slurm.qos`, validadas contra inyección de directivas; las reglas
  obligatorias del centro se comprueban antes de enviar.
- La partición por defecto la decide el perfil o el clúster (antes, `standard` fijo).
- El script lee el fichero de comandos con ruta relativa: vale igual en la copia remota.
- Panel web `slurmscope serve`, de solo lectura, con varios barridos y modo demo protegido.
- `cancel` cancela todos los job arrays activos del barrido.
- Corregido: el recuento por estado contaba dos veces las ejecuciones relanzadas.
- Agregado de métricas con mínimo y máximo.
- 133 casos de prueba; verificado en Hércules real.

## 1.0.0 — 2026-10-16

Primera versión de la obra. Incluye:

- Especificación de barrido en YAML o JSON (`sweep.yaml`), con validación estricta:
  campos obligatorios, tipos, claves desconocidas y plantillas que referencian parámetros
  inexistentes.
- Expansión del producto cartesiano en ejecuciones ordenadas, con clave única y validada
  (se rechazan plantillas que produzcan rutas absolutas o con `..`).
- Generación del fichero de comandos y del script de *job array* de SLURM (partición,
  tiempo, CPU, memoria, tope de paralelismo, entorno conda y `#SBATCH` adicionales).
- Creación preventiva de los directorios de logs y de resultados, y prefijo `mkdir -p` en
  los comandos para que ninguna ejecución se pierda por un directorio ausente.
- Envío y registro del barrido, y cancelación con volcado del resultado en el libro.
- Libro de ejecuciones en SQLite: envíos, tareas, estados normalizados, códigos de salida y
  duraciones.
- Sincronización idempotente con el clúster mediante `squeue` y `sacct`, con los fallos de
  lectura registrados como avisos visibles en vez de excepciones.
- Inspección del árbol de resultados (`metrics.json`, `result.json`, `summary.json`,
  `runs.csv`, `metrics.csv`, marcas `.done`/`FAILED`) y agregado de métricas numéricas.
- Estimación de tiempos con histórico real (mediana o percentil 90) o, en su defecto, con
  el límite solicitado del script: cómputo pendiente y tiempo de pared.
- Panel interactivo con siete secciones, desplazamiento, refresco y acciones con
  confirmación; degradación a menú de texto sin curses.
- Informes exportables en Markdown, JSON y CSV.
- Backend simulado determinista de SLURM para probar y demostrar el ciclo completo sin
  acceso a un HPC.
- Lector YAML propio, sin dependencias externas.
- 80 casos de prueba con la biblioteca estándar y una demostración reproducible
  (`examples/demo.sh`).
