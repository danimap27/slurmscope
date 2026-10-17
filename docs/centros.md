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

## Resumen

| Perfil | Máquina · centro | RES | Login | Obligatorio | Tiempo máx. | MaxArraySize | Sin internet |
|---|---|---|---|---|---|---|---|
| `hercules` | Hércules · CICA (Sevilla) |  | login.spc.cica.es | — | 7-00:00:00 | 500 |  |
| `picasso` | Picasso · SCBI, Universidad de Málaga | sí | picasso.scbi.uma.es | — | 7-00:00:00 | — |  |
| `albaicin` | Albaicín · CSIRC, Universidad de Granada |  | hpc.ugr.es | — | — | — |  |
| `marenostrum5-gpp` | MareNostrum 5 (GPP, CPU) · BSC-CNS (Barcelona) | sí | glogin1.bsc.es | account, qos | 3-00:00:00 | — | sí |
| `marenostrum5-acc` | MareNostrum 5 (ACC, GPU) · BSC-CNS (Barcelona) | sí | alogin1.bsc.es | account, qos | 3-00:00:00 | — | sí |
| `finisterrae3` | FinisTerrae III · CESGA (Santiago) | sí | ft3.cesga.es | QoS automática | 7-00:00:00 | — |  |
| `calendula` | Caléndula · SCAYLE (León) | sí | calendula.scayle.es | — | 45-00:00:00 | — | sí |
| `tirant` | Tirant · Universitat de València | sí | tirant.uv.es | QoS automática | 3-00:00:00 | — |  |
| `altamira` | Altamira · IFCA (Santander) | sí | login1.ifca.es | — | 3-00:00:00 | — |  |
| `lapalma` | LaPalma · IAC (La Palma) | sí | lapalma1.iac.es | QoS automática | 3-00:00:00 | — | sí |
| `teidehpc` | TeideHPC · ITER (Tenerife) | sí | hpclogin.hpc.iter.es | — | 3-00:00:00 | — |  |
| `pirineus3` | Pirineus III · CSUC (Barcelona) | sí | pirineus3.csuc.cat | — | — | — | sí |
| `agustina` | Agustina · BIFI, Universidad de Zaragoza | sí | agustina.bifi.unizar.es | — | 7-00:00:00 | — |  |
| `cibeles` | Cibeles · CCC, Universidad Autónoma de Madrid | sí | login1.ccc.uam.es | account, partición | — | — |  |
| `lusitania` | Lusitania · COMPUTAEX (Cáceres) | sí | — | — | — | — |  |
| `urederra` | Urederra · NASERTIC (Pamplona) | sí | — | — | — | — |  |
| `xula` | Xula · CIEMAT (Madrid) | sí | — | — | — | — |  |
| `turgalium` | Turgalium · CETA-CIEMAT (Trujillo) | sí | — | — | — | — |  |
| `dipc-hyperion` | Hyperion · DIPC (San Sebastián) |  | hyperion.sw.ehu.es | — | 8-00:00:00 | — |  |

Vacío = la documentación oficial no lo dice; `slurmscope probe` lo lee del clúster.

## Agradecimiento común a la RES

> The author thankfully acknowledges RES resources provided by [center] in [system] to [Activity ID].

Además hay que registrar la publicación en la RES Intranet ('Add publications') y enviar copia o enlace a dissemination@res.es (https://www.res.es/en/faq).

## Centros

### Hércules · CICA (Sevilla)

- **Login:** `login.spc.cica.es`, `login1.spc.cica.es`, `login2.spc.cica.es`, `login3.spc.cica.es`
- **Acceso:** VPN del CICA obligatoria; SSH con usuario y contraseña (o clave).
- **Partición:** standard
- **Dónde trabajar:** $HOME (Lustre, 1 TB, sin copia de seguridad: descarga los resultados)
- **Software:** module load Miniconda3 && source activate <entorno>

| QoS / partición | Tiempo máx. | A la vez | En cola | Nota |
|---|---|---|---|---|
| `standard` | 7-00:00:00 | — | — | 1-232 nodos, 96 tareas/nodo |
| `gpu` | — | — | — | 1-6 nodos, 1 A100 40 GB por nodo |

Normas y recomendaciones:

- /lustre es noexec en los nodos de login: instala y ejecuta vía SLURM.
- El Python del login es 3.6: SlurmScope se ejecuta en tu máquina por SSH.
- No uses 'conda activate' en scripts: 'source activate' tras el módulo.
- Sesiones interactivas: máximo 24 h y solo para pruebas.
- --time es obligatorio según el manual.

Fuentes: <https://www.cica.es/wp-content/uploads/2025/11/Manual-de-usuario-para-Cl%C3%BAster-H%C3%A9rcules-CICA.pdf>
Comprobado: 2026-10-17 · scontrol (SLURM 22.05.2, MaxArraySize=500) y manual.

### Picasso · SCBI, Universidad de Málaga · nodo RES

- **Login:** `picasso.scbi.uma.es`
- **Acceso:** SSH con usuario y contraseña; varios fallos bloquean la IP 30 min.
- **Partición:** la del clúster
- **Dónde trabajar:** $FSCRATCH para trabajar, $LOCALSCRATCH para E/S intensiva, resultados finales a $HOME
- **Software:** module load anaconda (pip install solo de lo que falte)
- **SlurmScope añade al script:** `#SBATCH --constraint=cal`

Normas y recomendaciones:

- El tipo de nodo se elige con --constraint (cal, intel, amd, dgx, blk), no con particiones.
- Más de 3 días de --time se considera arriesgado.
- Avisa a soporte antes del primer array de más de 1000 tareas o de trabajar con más de 15000 ficheros.
- Procesos de más de 10 min de CPU en el login se matan sin aviso.
- $FSCRATCH: ficheros sin acceso en 2 meses pueden borrarse.

Agradecimiento:

> The author thankfully acknowledges the computer resources (Picasso Supercomputer), technical expertise and assistance provided by the SCBI (Supercomputing and Bioinformatics) center of the University of Malaga.

Fuentes: <https://www.scbi.uma.es/web/resources/user_manual/>
Comprobado: manual oficial (consultado 2026-10-17).

### Albaicín · CSIRC, Universidad de Granada

- **Login:** `hpc.ugr.es`
- **Acceso:** Investigadores de universidades públicas andaluzas en grupos PAIDI.
- **Partición:** la del clúster
- **Dónde trabajar:** /SCRATCH/<grupo>/<usuario> (se borra a los ~100 días)

Normas y recomendaciones:

- Las publicaciones deben comunicarse en el portal de usuario.

Fuentes: <https://supercomputacion.ugr.es/manual/trabajos/>
Comprobado: manual oficial (consultado 2026-10-17).

### MareNostrum 5 (GPP, CPU) · BSC-CNS (Barcelona) · nodo RES

- **Login:** `glogin1.bsc.es`, `glogin2.bsc.es`
- **Acceso:** SSH; datos por transfer1-4.bsc.es. Usuario intransferible.
- **Partición:** la del clúster
- **QoS RES:** `gp_resb`
- **Dónde trabajar:** /gpfs/scratch/<grupo> o /gpfs/projects/<grupo>; $TMPDIR (NVMe local) para temporales
- **Internet:** sin conexiones salientes desde el clúster

| QoS / partición | Tiempo máx. | A la vez | En cola | Nota |
|---|---|---|---|---|
| `gp_debug` | 02:00:00 | — | — | — |
| `gp_interactive` | 02:00:00 | — | — | — |
| `gp_data` | 3-00:00:00 | — | — | — |
| `gp_hbm` | 3-00:00:00 | — | — | — |
| `gp_resa` | 3-00:00:00 | — | — | RES clase A |
| `gp_resb` | 2-00:00:00 | — | — | RES clase B |
| `gp_resc` | 1-00:00:00 | — | — | RES clase C |
| `gp_training` | 2-00:00:00 | — | — | — |
| `gp_ehpc` | 3-00:00:00 | — | — | — |

Normas y recomendaciones:

- Cuenta (grupo Unix del proyecto): 'bsc_project list'. QoS: 'bsc_queues'.
- La partición la determina la QoS.
- Procesos del login: límite de 5 min de CPU.
- Sin conexiones salientes: nada de pip/conda/git desde el clúster.
- Los jobs multinodo son exclusivos y se cobran nodos completos.
- Prueba el pipeline con gp_debug (2 h).

Agradecimiento:

> The author thankfully acknowledges RES resources provided by [center] in [system] to [Activity ID].

Fuentes: <https://www.bsc.es/supportkc/docs/MareNostrum5/slurm> · <https://www.bsc.es/supportkc/docs/MareNostrum5/storage> · <https://www.bsc.es/supportkc/FAQ>
Comprobado: guía oficial del BSC (consultada 2026-10-17).

### MareNostrum 5 (ACC, GPU) · BSC-CNS (Barcelona) · nodo RES

- **Login:** `alogin1.bsc.es`, `alogin2.bsc.es`
- **Acceso:** SSH; datos por transfer1-4.bsc.es. Usuario intransferible.
- **Partición:** la del clúster
- **QoS RES:** `acc_resb`
- **Dónde trabajar:** /gpfs/scratch/<grupo> o /gpfs/projects/<grupo>
- **Internet:** sin conexiones salientes desde el clúster

| QoS / partición | Tiempo máx. | A la vez | En cola | Nota |
|---|---|---|---|---|
| `acc_debug` | 02:00:00 | — | — | — |
| `acc_interactive` | 02:00:00 | — | — | — |
| `acc_resa` | 3-00:00:00 | — | — | RES clase A |
| `acc_resb` | 2-00:00:00 | — | — | RES clase B |
| `acc_resc` | 1-00:00:00 | — | — | RES clase C |
| `acc_training` | 2-00:00:00 | — | — | — |
| `acc_ehpc` | 3-00:00:00 | — | — | — |

Normas y recomendaciones:

- Por cada GPU hay que pedir 20 CPU (1 GPU-hora = 20 CPU-horas).
- GPU con extra_sbatch: '#SBATCH --gres=gpu:N' (1-4 por nodo).
- Sin conexiones salientes desde el clúster.

Agradecimiento:

> The author thankfully acknowledges RES resources provided by [center] in [system] to [Activity ID].

Fuentes: <https://www.bsc.es/supportkc/docs/MareNostrum5/slurm> · <https://www.bsc.es/supportkc/docs/MareNostrum5/overview>
Comprobado: guía oficial del BSC (consultada 2026-10-17).

### FinisTerrae III · CESGA (Santiago) · nodo RES

- **Login:** `ft3.cesga.es`
- **Acceso:** Desde fuera de centros autorizados, VPN Check Point (secure.cesga.es).
- **Partición:** la del clúster
- **Dónde trabajar:** $LUSTRE (1 TB, sin copia); resultados a $STORE
- **Software:** module load cesga/system miniconda3 (entornos en $STORE, nunca Miniconda propio)

| QoS / partición | Tiempo máx. | A la vez | En cola | Nota |
|---|---|---|---|---|
| `short` | 06:00:00 | 50 | 100 | — |
| `medium` | 3-00:00:00 | 30 | 50 | — |
| `long` | 7-00:00:00 | 5 | 10 | — |
| `class_a` | 3-00:00:00 | 40 | 100 | RES |
| `class_b` | 2-00:00:00 | 3 | 15 | RES |
| `class_c` | 1-00:00:00 | 1 | 5 | RES |

Normas y recomendaciones:

- La QoS se asigna sola: no la indiques salvo que tu proyecto RES lo pida.
- Cada tarea de un array cuenta como un trabajo para MaxSubmit.
- --mem y --time son obligatorios.
- Límites vigentes con 'batchlim'; uso real de memoria con 'seff <jobid>'.
- $HOME: 10 GB. Si se llena, fallan servicios.

Agradecimiento:

> Este trabajo de investigación se desarrolló gracias al acceso concedido por el Centro de Supercomputación de Galicia (CESGA) a sus infraestructuras de supercomputación. (Texto completo, con la financiación de FinisTerrae III, en https://cesga-docs.gitlab.io/ft3-user-guide/publicity.html)

Fuentes: <https://cesga-docs.gitlab.io/ft3-user-guide/batch_qos.html> · <https://cesga-docs.gitlab.io/ft3-user-guide/batch_job_array.html> · <https://cesga-docs.gitlab.io/ft3-user-guide/batch_memory.html>
Comprobado: guía oficial del CESGA (consultada 2026-10-17).

### Caléndula · SCAYLE (León) · nodo RES

- **Login:** `calendula.scayle.es`
- **Acceso:** SSH con clave ECDSA P-521 registrada por correo y contraseña; datos por dt.scayle.es.
- **Partición:** la del clúster · horas RES: `res`
- **QoS RES:** `class_a`
- **Dónde trabajar:** $SCRATCH (sin cuota, sin copia; borra lo no accedido en 45 días)
- **Internet:** sin conexiones salientes desde el clúster

| QoS / partición | Tiempo máx. | A la vez | En cola | Nota |
|---|---|---|---|---|
| `normal` | 5-00:00:00 | 150 | — | — |
| `long` | 15-00:00:00 | 50 | — | — |
| `xlong` | 30-00:00:00 | 25 | — | — |
| `xxlong` | 45-00:00:00 | 2 | — | — |

Normas y recomendaciones:

- Usuarios RES: --partition=res --qos=class_a.
- Ninguna actividad de cálculo en los frontends.
- No instales software ni scripts en $SCRATCH.
- Limita la concurrencia de los arrays con %N.
- Sin acceso al exterior desde el sistema.

Agradecimiento:

> The author thankfully acknowledges RES resources provided by [center] in [system] to [Activity ID].

Fuentes: <https://apps.scayle.es/manual/es/hpc/gestor-de-trabajos> · <https://apps.scayle.es/manual/es/hpc/usuario-res>
Comprobado: manual oficial de SCAYLE (consultado 2026-10-17).

### Tirant · Universitat de València · nodo RES

- **Login:** `tirant.uv.es`, `tirant2.uv.es`
- **Acceso:** SSH puerto 22 (cierre temporal desde fuera de la UV anunciado el 30-04-2026).
- **Partición:** la del clúster
- **Dónde trabajar:** /storage/scratch o /storage/projects/<grupo>; $TMPDIR, nunca /tmp

| QoS / partición | Tiempo máx. | A la vez | En cola | Nota |
|---|---|---|---|---|
| `class_a` | 3-00:00:00 | — | — | RES |
| `class_b` | 2-00:00:00 | — | — | RES |
| `class_c` | 1-00:00:00 | — | — | RES |

Normas y recomendaciones:

- Forma preferida: no indicar --qos (cola por defecto del usuario).
- --time obligatorio. No hay copia de seguridad de ningún sistema.
- El manual público es de Tirant 3; la máquina actual es Tirant 4.

Agradecimiento:

> The author thankfully acknowledges RES resources provided by [center] in [system] to [Activity ID].

Fuentes: <https://www.uv.es/tirant/Manuales/Tirant3_user_manual.pdf>
Comprobado: manual de Tirant 3 (consultado 2026-10-17).

### Altamira · IFCA (Santander) · nodo RES

- **Login:** `login1.ifca.es`, `login2.ifca.es`
- **Acceso:** SSH con 2FA (contraseña + OTP) o clave pública.
- **Partición:** la del clúster · horas RES: `res`
- **QoS RES:** `res_a`
- **SlurmScope añade al script:** `#SBATCH --export=NONE`
- **SlurmScope añade al script:** `source /etc/profile.d/modules.sh`

| QoS / partición | Tiempo máx. | A la vez | En cola | Nota |
|---|---|---|---|---|
| `main` | 3-00:00:00 | 1000 | 2000 | — |
| `res_a` | 3-00:00:00 | 500 | — | RES |
| `res_c` | 1-00:00:00 | 10 | 50 | RES sin horas |
| `debug` | 00:30:00 | 2 | — | — |
| `testing` | 00:30:00 | 2 | — | — |

Normas y recomendaciones:

- --export=NONE es obligatorio (sistema heterogéneo): el entorno se carga dentro del job.
- Usuarios RES: -p res -q res_a.
- Procesos de más de 10 min en el login se matan.
- Sin copias de seguridad.

Agradecimiento:

> We acknowledge Santander Supercomputacion support group at the University of Cantabria who provided access to the supercomputer Altamira Supercomputer at the Institute of Physics of Cantabria (IFCA-CSIC), member of the Spanish Supercomputing Network, for performing simulations/analyses.

Fuentes: <https://confluence.ifca.es/spaces/IC/pages/380895238/Running+jobs>
Comprobado: guía oficial del IFCA (consultada 2026-10-17).

### LaPalma · IAC (La Palma) · nodo RES

- **Login:** `lapalma1.iac.es`
- **Acceso:** SSH con 2FA opkssh ('opkssh login' antes de ssh).
- **Partición:** la del clúster
- **Dónde trabajar:** /storage/projects/<grupo>; $TMPDIR local, nunca /tmp
- **Internet:** sin conexiones salientes desde el clúster
- **SlurmScope añade al script:** `module purge`

| QoS / partición | Tiempo máx. | A la vez | En cola | Nota |
|---|---|---|---|---|
| `class_a` | 3-00:00:00 | — | — | — |
| `class_b` | 2-00:00:00 | — | — | — |
| `class_c` | 1-00:00:00 | — | — | — |
| `debug` | 00:30:00 | 1 | — | — |

Normas y recomendaciones:

- La QoS la asigna el comité de acceso: no hace falta indicarla.
- En arrays de programas secuenciales, aprovecha todos los cores del nodo.
- Login: 10 min de CPU. Sin copia de seguridad.

Agradecimiento:

> The author thankfully acknowledges the technical expertise and assistance provided by the Spanish Supercomputing Network (Red Española de Supercomputación), as well as the computer resources used: the LaPalma Supercomputer, located at the Instituto de Astrofísica de Canarias.

Fuentes: <https://research.iac.es/sieinvens/docs/supercomputing/lapalma/script_files.html>
Comprobado: guía oficial del IAC (consultada 2026-10-17).

### TeideHPC · ITER (Tenerife) · nodo RES

- **Login:** `hpclogin.hpc.iter.es`, `gpulogin.hpc.iter.es`
- **Acceso:** VPN OpenVPN obligatoria (una conexión simultánea).
- **Partición:** batch
- **Dónde trabajar:** ~/data (1 TB); /local/jobs para E/S intensiva
- **SlurmScope añade al script:** `module purge`

| QoS / partición | Tiempo máx. | A la vez | En cola | Nota |
|---|---|---|---|---|
| `express` | 03:00:00 | — | — | — |
| `batch` | 1-00:00:00 | — | — | — |
| `long` | 3-00:00:00 | — | — | — |

Normas y recomendaciones:

- Nodos compartidos: por defecto 1 CPU y 2 GB; reserva explícitamente.
- Usa srun para lanzar la aplicación.
- $HOME: 5 GB, no guardes datos ni código.

Agradecimiento:

> The author thankfully acknowledges RES resources provided by [center] in [system] to [Activity ID].

Fuentes: <https://doc.hpc.iter.es/latest/en/slurm/how_to_slurm.html>
Comprobado: guía oficial de TeideHPC (consultada 2026-10-17).

### Pirineus III · CSUC (Barcelona) · nodo RES

- **Login:** `pirineus3.csuc.cat`
- **Acceso:** SSH por el puerto 2122: cluster.ssh_options: ['-p', '2122'].
- **Partición:** la del clúster · horas RES: `res_cpu`
- **Dónde trabajar:** $SCRATCH durante el job (se borra 7 días después), resultados a /data
- **Internet:** sin conexiones salientes desde el clúster

Normas y recomendaciones:

- Usuarios RES: partición res_cpu o res_gpu.
- --time obligatorio.
- Pide 1-3 GB más de memoria de la que use el programa.

Agradecimiento:

> This project was carried out with the use of CSUC supercomputing resources.

Fuentes: <https://confluence.csuc.cat/spaces/HPCKB/pages/48037919/Red+Espa%C3%B1ola+de+Supercomputaci%C3%B3n>
Comprobado: base de conocimiento del CSUC (consultada 2026-10-17).

### Agustina · BIFI, Universidad de Zaragoza · nodo RES

- **Login:** `agustina.bifi.unizar.es`
- **Acceso:** SSH directo si tu IP está abierta; si no, por bridge.bifi.unizar.es (cluster.ssh_options: ['-J', 'usuario@bridge.bifi.unizar.es']).
- **Partición:** agustina_thin · horas RES: `RES`

Normas y recomendaciones:

- Con horas RES, la partición RES es obligatoria.
- Los trabajos lanzados en el login se cancelan.

Agradecimiento:

> The author thankfully acknowledges RES resources provided by [center] in [system] to [Activity ID].

Fuentes: <https://doc-publica.bifi.es/print.html>
Comprobado: documentación pública del BIFI (consultada 2026-10-17).

### Cibeles · CCC, Universidad Autónoma de Madrid · nodo RES

- **Login:** `login1.ccc.uam.es`, `login2.ccc.uam.es`, `login3.ccc.uam.es`
- **Acceso:** Solo desde IP de la UAM o IP estática habilitada (usuarios RES).
- **Partición:** la del clúster

Normas y recomendaciones:

- Proyecto (-A) y cola (-p) obligatorios: 'projects' los lista.
- No mezcles nodos Cibeles3 y Cibeles4 en trabajos MPI.

Agradecimiento:

> We acknowledge the computer resources and assistance provided by Centro de Computación Científica-Universidad Autónoma de Madrid (CCC-UAM).

Fuentes: <https://ccc.uam.es/en/documentation/FAQ_en.html>
Comprobado: guías del CCC-UAM (consultadas 2026-10-17).

### Lusitania · COMPUTAEX (Cáceres) · nodo RES

- **Partición:** la del clúster

Normas y recomendaciones:

- Sin documentación pública de usuario: pídela a res_support@computaex.es y usa 'slurmscope probe'.

Agradecimiento:

> The author thankfully acknowledges RES resources provided by [center] in [system] to [Activity ID].

Fuentes: <https://www.res.es/en/about-res/nodes/lusitania-cenits>

### Urederra · NASERTIC (Pamplona) · nodo RES

- **Partición:** la del clúster

Normas y recomendaciones:

- Sin guía pública de usuario: pide información en https://hpc.nasertic.es/es/solicitud-de-informacion y usa 'slurmscope probe'.
- Ficha RES: 180 nodos (Skylake y Broadwell), scratch Lustre, Rocky Linux 9.

Agradecimiento:

> The author thankfully acknowledges RES resources provided by [center] in [system] to [Activity ID].

Fuentes: <https://www.res.es/en/about-res/nodes/urederra-nasertic>
Comprobado: ficha del nodo en res.es (consultada 2026-10-17).

### Xula · CIEMAT (Madrid) · nodo RES

- **Partición:** la del clúster

Normas y recomendaciones:

- Sin guía pública de usuario: soporte en res_support@ciemat.es; usa 'slurmscope probe'.
- Ficha RES: 144 nodos Xeon Platinum 8160 (48 cores, 96 GB), Lustre, Rocky 8.

Agradecimiento:

> The author thankfully acknowledges RES resources provided by [center] in [system] to [Activity ID].

Fuentes: <https://www.res.es/en/about-res/nodes/xula-turgalium-ciemat>
Comprobado: ficha del nodo en res.es (consultada 2026-10-17).

### Turgalium · CETA-CIEMAT (Trujillo) · nodo RES

- **Partición:** la del clúster

Normas y recomendaciones:

- Sin guía pública de usuario: soporte en res_support@ceta-ciemat.es; usa 'slurmscope probe'.
- Ficha RES: 144 nodos Xeon Platinum 8160 (48 cores, 96 GB), Lustre, Rocky 8.

Agradecimiento:

> The author thankfully acknowledges RES resources provided by [center] in [system] to [Activity ID].

Fuentes: <https://www.res.es/en/about-res/nodes/xula-turgalium-ciemat>
Comprobado: ficha del nodo en res.es (consultada 2026-10-17).

### Hyperion · DIPC (San Sebastián)

- **Login:** `hyperion.sw.ehu.es`
- **Acceso:** SSH con clave registrada en account-setup.sw.ehu.es.
- **Partición:** la del clúster
- **Dónde trabajar:** /scratch

| QoS / partición | Tiempo máx. | A la vez | En cola | Nota |
|---|---|---|---|---|
| `regular` | 1-00:00:00 | 180 | — | — |
| `test` | 00:10:00 | 2 | 2 | — |
| `long` | 2-00:00:00 | 40 | — | — |
| `xlong` | 8-00:00:00 | 20 | 200 | — |
| `serial` | 2-00:00:00 | 1000 | 2000 | — |

Normas y recomendaciones:

- Máximo 1000 trabajos en cola por usuario sumando todas las QoS.
- Un proceso intensivo en el login suspende la cuenta.

Fuentes: <https://scc.dipc.org/docs/jobs/slurm>
Comprobado: documentación del DIPC (consultada 2026-10-17).
