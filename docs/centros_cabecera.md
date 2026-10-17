# Normas y buenas prácticas de los superordenadores

Resumen de la documentación oficial de los 14 nodos de la Red Española de
Supercomputación (RES), de Hércules (CICA) y de otros dos clústeres académicos
(Albaicín, DIPC). PIC (UAB) es un nodo solo de datos, sin ejecución de trabajos, y no
tiene perfil. Urederra, Xula, Turgalium y Lusitania no publican guía de usuario: sus
perfiles son mínimos y remiten a `slurmscope probe` y a su soporte. Revisión: 17-10-2026. **Este fichero se genera** con
`python3 scripts/gen_centros.py` a partir de `slurmscope/clusters_data.py`: lo que se lee
aquí es exactamente lo que SlurmScope aplica.

Los límites cambian. Los centros piden consultarlos en el propio sistema (`bsc_queues`,
`batchlim`, `sacctmgr show qos`…), y por eso `slurmscope probe` los lee del clúster y
prevalecen sobre este resumen.

## Lo que comparten todos

1. **Todo el cálculo pasa por SLURM.** Los nodos de login son para editar, compilar y
   enviar. Varios centros matan los procesos largos del login: MareNostrum 5 a los 5 min
   de CPU, Picasso, Altamira y LaPalma a los 10 min. DIPC suspende la cuenta.
2. **`--time` siempre, ajustado y por debajo del máximo de la QoS o partición.** Es
   obligatorio en Hércules, FinisTerrae III, Caléndula, Tirant y Pirineus. Un `--time`
   ajustado entra antes en cola.
3. **Pide la memoria y las CPU que vas a usar** y revísalas al terminar con `seff <jobid>`.
   FinisTerrae III exige `--mem`. En TeideHPC, sin pedirla, solo tienes 2 GB.
4. **Arrays para barridos, con la concurrencia limitada (`%N`).** En SLURM **cada tarea de
   un array cuenta como un trabajo** para `MaxSubmit`. FinisTerrae III lo documenta: con
   `class_b`, como mucho 15 en cola. Picasso pide avisar antes de arrays de más de 1000 tareas.
5. **Nada de cálculo ni datos en `$HOME`, y nunca `/tmp`.** Se trabaja en el scratch del
   centro (`/gpfs/scratch`, `$LUSTRE`, `$FSCRATCH`, `$SCRATCH`…). La E/S intensiva va al
   disco local del nodo (`$TMPDIR`, `$LOCALSCRATCH`), y los resultados se copian a un
   sitio persistente.
6. **El scratch se purga y casi nada tiene copia de seguridad.** Caléndula purga a los 45
   días, Picasso a los 2 meses, Albaicín a los ~100 días y Pirineus a los 7 días de
   terminar el job. Tirant, LaPalma, Altamira y MareNostrum 5 (scratch) no tienen copia.
   Descarga los resultados.
7. **Muchos centros no tienen salida a internet** (MareNostrum 5, Caléndula, LaPalma,
   Pirineus). Prepara el entorno antes, o con los módulos del centro, y sube los datos
   desde tu máquina.
8. **Software con módulos del centro.** Conda, si lo hay, siempre con el módulo oficial
   (FinisTerrae III prohíbe instalar Miniconda propio y pide los entornos en `$STORE`).
   En Hércules no uses `conda activate` en scripts.
9. **Cuentas personales e intransferibles.** Muchos centros usan 2FA o claves específicas:
   Altamira (OTP), LaPalma (opkssh), Caléndula (ECDSA P-521). Muchos exigen VPN (Hércules,
   FinisTerrae III fuera de centros autorizados, TeideHPC) o una IP autorizada
   (Agustina, Cibeles).
10. **Agradecimiento obligatorio en las publicaciones** y registro en la RES Intranet.

## Cómo lo aplica SlurmScope

- `slurmscope check` contrasta la especificación con estas normas, y `submit` **no envía
  nada si hay errores**. Se comprueba:
  - la cuenta, la QoS y la partición obligatorias;
  - `--time` frente al máximo de la QoS o partición, incluida la asignación automática
    por tiempo de FinisTerrae III;
  - 20 CPU por GPU en MareNostrum 5 ACC;
  - que no haya descargas donde no hay internet;
  - que el directorio de trabajo no esté en `$HOME` donde está desaconsejado;
  - el `MaxArraySize` real del clúster.
- **Oleadas por `MaxSubmit`.** `submit` envía solo lo que cabe en la cola, y
  `slurmscope resume` envía lo pendiente (`--failed` relanza solo lo fallido). Los job
  arrays se trocean por `MaxArraySize` con listas de índices.
- **El script sigue lo que exige cada centro:**
  - `--export=NONE` y `source /etc/profile.d/modules.sh` en Altamira;
  - `--constraint=cal` en Picasso;
  - `module purge` en LaPalma y TeideHPC;
  - `OMP_NUM_THREADS` y `SRUN_CPUS_PER_TASK` iguales a las CPU pedidas;
  - correo opcional con `slurm.mail_user` y `slurm.mail_type`.
- `cluster.allocation: res` aplica la partición y la QoS de la RES de cada centro
  (Caléndula `res`/`class_a`, Altamira `res`/`res_a`, Pirineus `res_cpu`, Agustina `RES`,
  MareNostrum 5 `gp_resb`/`acc_resb`), salvo donde la QoS es automática.
- `cluster.remote_dir` admite las variables del centro (`$LUSTRE/...`, `$FSCRATCH/...`):
  se resuelven en el propio clúster.
- `slurmscope acknowledge` imprime el texto de agradecimiento que exige el centro.
