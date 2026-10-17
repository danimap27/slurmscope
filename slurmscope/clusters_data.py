"""Reglas de los centros de supercomputación, extraídas de su documentación oficial.

Cada perfil recoge solo datos publicados por el centro (o comprobados en el propio
clúster), con la fuente en ``docs``. Lo que la documentación no dice se deja vacío
a propósito: ``slurmscope probe`` lo lee del clúster y prevalece sobre el perfil.

Revisión documental: 17-10-2026. Los límites cambian: los centros piden
consultarlos en el sistema (``bsc_queues``, ``batchlim``, ``sacctmgr show qos``…).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

RES_ACKNOWLEDGEMENT = (
    "The author thankfully acknowledges RES resources provided by [center] in [system] "
    "to [Activity ID]."
)
RES_ACKNOWLEDGEMENT_NOTE = (
    "Además hay que registrar la publicación en la RES Intranet ('Add publications') y "
    "enviar copia o enlace a dissemination@res.es (https://www.res.es/en/faq)."
)


@dataclass(frozen=True)
class Limits:
    """Límites de una QoS o partición. ``None`` = no documentado."""

    max_wall: Optional[str] = None
    max_jobs: Optional[int] = None      # MaxJobsPU: tareas en ejecución a la vez
    max_submit: Optional[int] = None    # MaxSubmitPU: tareas en cola + ejecución
    note: str = ""


@dataclass(frozen=True)
class ClusterProfile:
    """Reglas conocidas de un centro de supercomputación."""

    name: str
    title: str
    centre: str
    res_node: bool = False
    logins: Tuple[str, ...] = ()
    access: str = ""
    partition: Optional[str] = None            # partición por defecto a pedir
    res_partition: Optional[str] = None        # partición para horas RES
    res_qos: Optional[str] = None              # QoS para horas RES
    max_array_size: Optional[int] = None
    requires_account: bool = False
    requires_qos: bool = False
    requires_partition: bool = False
    qos_is_automatic: bool = False             # el centro asigna la QoS: no indicarla
    qos_limits: Dict[str, Limits] = field(default_factory=dict)
    partition_limits: Dict[str, Limits] = field(default_factory=dict)
    # QoS o partición que el centro asigna según el --time pedido (de menor a mayor)
    time_tiers: Tuple[str, ...] = ()
    max_wall: Optional[str] = None             # límite general si no hay más detalle
    max_submit_total: Optional[int] = None     # tope global de trabajos por usuario
    support_review_above: Optional[int] = None  # avisar a soporte por encima de N tareas
    internet_from_cluster: Optional[bool] = None
    home_for_jobs: Optional[bool] = None       # False = no ejecutar trabajos desde $HOME
    workdir_hint: str = ""
    directives: Tuple[str, ...] = ()           # directivas #SBATCH que exige el centro
    preamble: Tuple[str, ...] = ()             # órdenes al principio del job
    cpus_per_gpu: Optional[int] = None
    setup_hint: str = ""
    notes: Tuple[str, ...] = ()
    acknowledgement: Optional[str] = None
    docs: Tuple[str, ...] = ()
    verified: str = ""

    @property
    def login(self) -> Optional[str]:
        return self.logins[0] if self.logins else None

    @property
    def qos_examples(self) -> Tuple[str, ...]:
        return tuple(self.qos_limits)

    def as_dict(self) -> Dict[str, object]:
        def limits(table: Dict[str, Limits]) -> Dict[str, Dict[str, object]]:
            return {k: {"max_wall": v.max_wall, "max_jobs": v.max_jobs,
                        "max_submit": v.max_submit, "note": v.note}
                    for k, v in table.items()}

        return {
            "name": self.name, "title": self.title, "centre": self.centre,
            "res_node": self.res_node, "logins": list(self.logins), "access": self.access,
            "partition": self.partition, "res_partition": self.res_partition,
            "res_qos": self.res_qos, "max_array_size": self.max_array_size,
            "requires_account": self.requires_account, "requires_qos": self.requires_qos,
            "requires_partition": self.requires_partition,
            "qos_is_automatic": self.qos_is_automatic,
            "qos_limits": limits(self.qos_limits),
            "partition_limits": limits(self.partition_limits),
            "time_tiers": list(self.time_tiers), "max_wall": self.max_wall,
            "max_submit_total": self.max_submit_total,
            "support_review_above": self.support_review_above,
            "internet_from_cluster": self.internet_from_cluster,
            "home_for_jobs": self.home_for_jobs, "workdir_hint": self.workdir_hint,
            "directives": list(self.directives), "preamble": list(self.preamble),
            "cpus_per_gpu": self.cpus_per_gpu, "setup_hint": self.setup_hint,
            "notes": list(self.notes), "acknowledgement": self.acknowledgement,
            "docs": list(self.docs), "verified": self.verified,
        }


L = Limits

PROFILES: Dict[str, ClusterProfile] = {
    # ───────────────────────────── Andalucía ─────────────────────────────────
    "hercules": ClusterProfile(
        name="hercules", title="Hércules", centre="CICA (Sevilla)",
        logins=("login.spc.cica.es", "login1.spc.cica.es", "login2.spc.cica.es",
                "login3.spc.cica.es"),
        access="VPN del CICA obligatoria; SSH con usuario y contraseña (o clave).",
        partition="standard", max_array_size=500,
        partition_limits={"standard": L("7-00:00:00", note="1-232 nodos, 96 tareas/nodo"),
                          "gpu": L(note="1-6 nodos, 1 A100 40 GB por nodo")},
        home_for_jobs=True,
        workdir_hint="$HOME (Lustre, 1 TB, sin copia de seguridad: descarga los resultados)",
        setup_hint="module load Miniconda3 && source activate <entorno>",
        notes=(
            "/lustre es noexec en los nodos de login: instala y ejecuta vía SLURM.",
            "El Python del login es 3.6: SlurmScope se ejecuta en tu máquina por SSH.",
            "No uses 'conda activate' en scripts: 'source activate' tras el módulo.",
            "Sesiones interactivas: máximo 24 h y solo para pruebas.",
            "--time es obligatorio según el manual.",
        ),
        docs=("https://www.cica.es/wp-content/uploads/2025/11/"
              "Manual-de-usuario-para-Cl%C3%BAster-H%C3%A9rcules-CICA.pdf",),
        verified="2026-10-17 · scontrol (SLURM 22.05.2, MaxArraySize=500) y manual",
    ),
    "picasso": ClusterProfile(
        name="picasso", title="Picasso", centre="SCBI, Universidad de Málaga", res_node=True,
        logins=("picasso.scbi.uma.es",),
        access="SSH con usuario y contraseña; varios fallos bloquean la IP 30 min.",
        max_wall="7-00:00:00", support_review_above=1000,
        home_for_jobs=False,
        workdir_hint="$FSCRATCH para trabajar, $LOCALSCRATCH para E/S intensiva, "
                     "resultados finales a $HOME",
        directives=("#SBATCH --constraint=cal",),
        setup_hint="module load anaconda (pip install solo de lo que falte)",
        notes=(
            "El tipo de nodo se elige con --constraint (cal, intel, amd, dgx, blk), "
            "no con particiones.",
            "Más de 3 días de --time se considera arriesgado.",
            "Avisa a soporte antes del primer array de más de 1000 tareas o de "
            "trabajar con más de 15000 ficheros.",
            "Procesos de más de 10 min de CPU en el login se matan sin aviso.",
            "$FSCRATCH: ficheros sin acceso en 2 meses pueden borrarse.",
        ),
        acknowledgement=(
            "The author thankfully acknowledges the computer resources (Picasso "
            "Supercomputer), technical expertise and assistance provided by the SCBI "
            "(Supercomputing and Bioinformatics) center of the University of Malaga."
        ),
        docs=("https://www.scbi.uma.es/web/resources/user_manual/",),
        verified="manual oficial (consultado 2026-10-17)",
    ),
    "albaicin": ClusterProfile(
        name="albaicin", title="Albaicín", centre="CSIRC, Universidad de Granada",
        logins=("hpc.ugr.es",),
        access="Investigadores de universidades públicas andaluzas en grupos PAIDI.",
        home_for_jobs=False,
        workdir_hint="/SCRATCH/<grupo>/<usuario> (se borra a los ~100 días)",
        notes=("Las publicaciones deben comunicarse en el portal de usuario.",),
        docs=("https://supercomputacion.ugr.es/manual/trabajos/",),
        verified="manual oficial (consultado 2026-10-17)",
    ),
    # ───────────────────────────── BSC ───────────────────────────────────────
    "marenostrum5-gpp": ClusterProfile(
        name="marenostrum5-gpp", title="MareNostrum 5 (GPP, CPU)", centre="BSC-CNS (Barcelona)",
        res_node=True, logins=("glogin1.bsc.es", "glogin2.bsc.es"),
        access="SSH; datos por transfer1-4.bsc.es. Usuario intransferible.",
        requires_account=True, requires_qos=True, res_qos="gp_resb",
        qos_limits={
            "gp_debug": L("02:00:00"), "gp_interactive": L("02:00:00"),
            "gp_data": L("3-00:00:00"), "gp_hbm": L("3-00:00:00"),
            "gp_resa": L("3-00:00:00", note="RES clase A"),
            "gp_resb": L("2-00:00:00", note="RES clase B"),
            "gp_resc": L("1-00:00:00", note="RES clase C"),
            "gp_training": L("2-00:00:00"), "gp_ehpc": L("3-00:00:00"),
        },
        internet_from_cluster=False, home_for_jobs=False,
        workdir_hint="/gpfs/scratch/<grupo> o /gpfs/projects/<grupo>; $TMPDIR (NVMe local) "
                     "para temporales",
        notes=(
            "Cuenta (grupo Unix del proyecto): 'bsc_project list'. QoS: 'bsc_queues'.",
            "La partición la determina la QoS.",
            "Procesos del login: límite de 5 min de CPU.",
            "Sin conexiones salientes: nada de pip/conda/git desde el clúster.",
            "Los jobs multinodo son exclusivos y se cobran nodos completos.",
            "Prueba el pipeline con gp_debug (2 h).",
        ),
        acknowledgement=RES_ACKNOWLEDGEMENT,
        docs=("https://www.bsc.es/supportkc/docs/MareNostrum5/slurm",
              "https://www.bsc.es/supportkc/docs/MareNostrum5/storage",
              "https://www.bsc.es/supportkc/FAQ"),
        verified="guía oficial del BSC (consultada 2026-10-17)",
    ),
    "marenostrum5-acc": ClusterProfile(
        name="marenostrum5-acc", title="MareNostrum 5 (ACC, GPU)", centre="BSC-CNS (Barcelona)",
        res_node=True, logins=("alogin1.bsc.es", "alogin2.bsc.es"),
        access="SSH; datos por transfer1-4.bsc.es. Usuario intransferible.",
        requires_account=True, requires_qos=True, res_qos="acc_resb",
        qos_limits={
            "acc_debug": L("02:00:00"), "acc_interactive": L("02:00:00"),
            "acc_resa": L("3-00:00:00", note="RES clase A"),
            "acc_resb": L("2-00:00:00", note="RES clase B"),
            "acc_resc": L("1-00:00:00", note="RES clase C"),
            "acc_training": L("2-00:00:00"), "acc_ehpc": L("3-00:00:00"),
        },
        internet_from_cluster=False, home_for_jobs=False,
        workdir_hint="/gpfs/scratch/<grupo> o /gpfs/projects/<grupo>",
        cpus_per_gpu=20,
        notes=(
            "Por cada GPU hay que pedir 20 CPU (1 GPU-hora = 20 CPU-horas).",
            "GPU con extra_sbatch: '#SBATCH --gres=gpu:N' (1-4 por nodo).",
            "Sin conexiones salientes desde el clúster.",
        ),
        acknowledgement=RES_ACKNOWLEDGEMENT,
        docs=("https://www.bsc.es/supportkc/docs/MareNostrum5/slurm",
              "https://www.bsc.es/supportkc/docs/MareNostrum5/overview"),
        verified="guía oficial del BSC (consultada 2026-10-17)",
    ),
    # ───────────────────────────── resto de la RES ───────────────────────────
    "finisterrae3": ClusterProfile(
        name="finisterrae3", title="FinisTerrae III", centre="CESGA (Santiago)", res_node=True,
        logins=("ft3.cesga.es",),
        access="Desde fuera de centros autorizados, VPN Check Point (secure.cesga.es).",
        qos_is_automatic=True, res_qos="class_a",
        qos_limits={
            "short": L("06:00:00", 50, 100), "medium": L("3-00:00:00", 30, 50),
            "long": L("7-00:00:00", 5, 10),
            "class_a": L("3-00:00:00", 40, 100, "RES"),
            "class_b": L("2-00:00:00", 3, 15, "RES"),
            "class_c": L("1-00:00:00", 1, 5, "RES"),
        },
        time_tiers=("short", "medium", "long"),
        home_for_jobs=False, workdir_hint="$LUSTRE (1 TB, sin copia); resultados a $STORE",
        setup_hint="module load cesga/system miniconda3 (entornos en $STORE, nunca "
                   "Miniconda propio)",
        notes=(
            "La QoS se asigna sola: no la indiques salvo que tu proyecto RES lo pida.",
            "Cada tarea de un array cuenta como un trabajo para MaxSubmit.",
            "--mem y --time son obligatorios.",
            "Límites vigentes con 'batchlim'; uso real de memoria con 'seff <jobid>'.",
            "$HOME: 10 GB. Si se llena, fallan servicios.",
        ),
        acknowledgement=(
            "Este trabajo de investigación se desarrolló gracias al acceso concedido por "
            "el Centro de Supercomputación de Galicia (CESGA) a sus infraestructuras de "
            "supercomputación. (Texto completo, con la financiación de FinisTerrae III, en "
            "https://cesga-docs.gitlab.io/ft3-user-guide/publicity.html)"
        ),
        docs=("https://cesga-docs.gitlab.io/ft3-user-guide/batch_qos.html",
              "https://cesga-docs.gitlab.io/ft3-user-guide/batch_job_array.html",
              "https://cesga-docs.gitlab.io/ft3-user-guide/batch_memory.html"),
        verified="guía oficial del CESGA (consultada 2026-10-17)",
    ),
    "calendula": ClusterProfile(
        name="calendula", title="Caléndula", centre="SCAYLE (León)", res_node=True,
        logins=("calendula.scayle.es",),
        access="SSH con clave ECDSA P-521 registrada por correo y contraseña; datos por "
               "dt.scayle.es.",
        res_partition="res", res_qos="class_a",
        qos_limits={"normal": L("5-00:00:00", 150), "long": L("15-00:00:00", 50),
                    "xlong": L("30-00:00:00", 25), "xxlong": L("45-00:00:00", 2)},
        internet_from_cluster=False, home_for_jobs=False,
        workdir_hint="$SCRATCH (sin cuota, sin copia; borra lo no accedido en 45 días)",
        notes=(
            "Usuarios RES: --partition=res --qos=class_a.",
            "Ninguna actividad de cálculo en los frontends.",
            "No instales software ni scripts en $SCRATCH.",
            "Limita la concurrencia de los arrays con %N.",
            "Sin acceso al exterior desde el sistema.",
        ),
        acknowledgement=RES_ACKNOWLEDGEMENT,
        docs=("https://apps.scayle.es/manual/es/hpc/gestor-de-trabajos",
              "https://apps.scayle.es/manual/es/hpc/usuario-res"),
        verified="manual oficial de SCAYLE (consultado 2026-10-17)",
    ),
    "tirant": ClusterProfile(
        name="tirant", title="Tirant", centre="Universitat de València", res_node=True,
        logins=("tirant.uv.es", "tirant2.uv.es"),
        access="SSH puerto 22 (cierre temporal desde fuera de la UV anunciado el 30-04-2026).",
        qos_is_automatic=True,
        qos_limits={"class_a": L("3-00:00:00", note="RES"), "class_b": L("2-00:00:00", note="RES"),
                    "class_c": L("1-00:00:00", note="RES")},
        home_for_jobs=False,
        workdir_hint="/storage/scratch o /storage/projects/<grupo>; $TMPDIR, nunca /tmp",
        notes=(
            "Forma preferida: no indicar --qos (cola por defecto del usuario).",
            "--time obligatorio. No hay copia de seguridad de ningún sistema.",
            "El manual público es de Tirant 3; la máquina actual es Tirant 4.",
        ),
        acknowledgement=RES_ACKNOWLEDGEMENT,
        docs=("https://www.uv.es/tirant/Manuales/Tirant3_user_manual.pdf",),
        verified="manual de Tirant 3 (consultado 2026-10-17)",
    ),
    "altamira": ClusterProfile(
        name="altamira", title="Altamira", centre="IFCA (Santander)", res_node=True,
        logins=("login1.ifca.es", "login2.ifca.es"),
        access="SSH con 2FA (contraseña + OTP) o clave pública.",
        res_partition="res", res_qos="res_a",
        qos_limits={
            "main": L("3-00:00:00", 1000, 2000), "res_a": L("3-00:00:00", 500, note="RES"),
            "res_c": L("1-00:00:00", 10, 50, "RES sin horas"),
            "debug": L("00:30:00", 2), "testing": L("00:30:00", 2),
        },
        directives=("#SBATCH --export=NONE",),
        preamble=("source /etc/profile.d/modules.sh",),
        notes=(
            "--export=NONE es obligatorio (sistema heterogéneo): el entorno se carga "
            "dentro del job.",
            "Usuarios RES: -p res -q res_a.",
            "Procesos de más de 10 min en el login se matan.",
            "Sin copias de seguridad.",
        ),
        acknowledgement=(
            "We acknowledge Santander Supercomputacion support group at the University of "
            "Cantabria who provided access to the supercomputer Altamira Supercomputer at "
            "the Institute of Physics of Cantabria (IFCA-CSIC), member of the Spanish "
            "Supercomputing Network, for performing simulations/analyses."
        ),
        docs=("https://confluence.ifca.es/spaces/IC/pages/380895238/Running+jobs",),
        verified="guía oficial del IFCA (consultada 2026-10-17)",
    ),
    "lapalma": ClusterProfile(
        name="lapalma", title="LaPalma", centre="IAC (La Palma)", res_node=True,
        logins=("lapalma1.iac.es",),
        access="SSH con 2FA opkssh ('opkssh login' antes de ssh).",
        qos_is_automatic=True,
        qos_limits={"class_a": L("3-00:00:00"), "class_b": L("2-00:00:00"),
                    "class_c": L("1-00:00:00"), "debug": L("00:30:00", 1)},
        internet_from_cluster=False, home_for_jobs=False,
        workdir_hint="/storage/projects/<grupo>; $TMPDIR local, nunca /tmp",
        preamble=("module purge",),
        notes=(
            "La QoS la asigna el comité de acceso: no hace falta indicarla.",
            "En arrays de programas secuenciales, aprovecha todos los cores del nodo.",
            "Login: 10 min de CPU. Sin copia de seguridad.",
        ),
        acknowledgement=(
            "The author thankfully acknowledges the technical expertise and assistance "
            "provided by the Spanish Supercomputing Network (Red Española de "
            "Supercomputación), as well as the computer resources used: the LaPalma "
            "Supercomputer, located at the Instituto de Astrofísica de Canarias."
        ),
        docs=("https://research.iac.es/sieinvens/docs/supercomputing/lapalma/script_files.html",),
        verified="guía oficial del IAC (consultada 2026-10-17)",
    ),
    "teidehpc": ClusterProfile(
        name="teidehpc", title="TeideHPC", centre="ITER (Tenerife)", res_node=True,
        logins=("hpclogin.hpc.iter.es", "gpulogin.hpc.iter.es"),
        access="VPN OpenVPN obligatoria (una conexión simultánea).",
        partition="batch",
        partition_limits={"express": L("03:00:00"), "batch": L("1-00:00:00"),
                          "long": L("3-00:00:00")},
        home_for_jobs=False, workdir_hint="~/data (1 TB); /local/jobs para E/S intensiva",
        preamble=("module purge",),
        notes=("Nodos compartidos: por defecto 1 CPU y 2 GB; reserva explícitamente.",
               "Usa srun para lanzar la aplicación.",
               "$HOME: 5 GB, no guardes datos ni código."),
        acknowledgement=RES_ACKNOWLEDGEMENT,
        docs=("https://doc.hpc.iter.es/latest/en/slurm/how_to_slurm.html",),
        verified="guía oficial de TeideHPC (consultada 2026-10-17)",
    ),
    "pirineus3": ClusterProfile(
        name="pirineus3", title="Pirineus III", centre="CSUC (Barcelona)", res_node=True,
        logins=("pirineus3.csuc.cat",),
        access="SSH por el puerto 2122: cluster.ssh_options: ['-p', '2122'].",
        res_partition="res_cpu", internet_from_cluster=False,
        workdir_hint="$SCRATCH durante el job (se borra 7 días después), resultados a /data",
        notes=("Usuarios RES: partición res_cpu o res_gpu.",
               "--time obligatorio.",
               "Pide 1-3 GB más de memoria de la que use el programa."),
        acknowledgement="This project was carried out with the use of CSUC supercomputing "
                        "resources.",
        docs=("https://confluence.csuc.cat/spaces/HPCKB/pages/48037919/Red+Espa%C3%B1ola+de+Supercomputaci%C3%B3n",),
        verified="base de conocimiento del CSUC (consultada 2026-10-17)",
    ),
    "agustina": ClusterProfile(
        name="agustina", title="Agustina", centre="BIFI, Universidad de Zaragoza", res_node=True,
        logins=("agustina.bifi.unizar.es",),
        access="SSH directo si tu IP está abierta; si no, por bridge.bifi.unizar.es "
               "(cluster.ssh_options: ['-J', 'usuario@bridge.bifi.unizar.es']).",
        partition="agustina_thin", res_partition="RES", max_wall="7-00:00:00",
        notes=("Con horas RES, la partición RES es obligatoria.",
               "Los trabajos lanzados en el login se cancelan."),
        acknowledgement=RES_ACKNOWLEDGEMENT,
        docs=("https://doc-publica.bifi.es/print.html",),
        verified="documentación pública del BIFI (consultada 2026-10-17)",
    ),
    "cibeles": ClusterProfile(
        name="cibeles", title="Cibeles", centre="CCC, Universidad Autónoma de Madrid",
        res_node=True, logins=("login1.ccc.uam.es", "login2.ccc.uam.es", "login3.ccc.uam.es"),
        access="Solo desde IP de la UAM o IP estática habilitada (usuarios RES).",
        requires_account=True, requires_partition=True,
        notes=("Proyecto (-A) y cola (-p) obligatorios: 'projects' los lista.",
               "No mezcles nodos Cibeles3 y Cibeles4 en trabajos MPI."),
        acknowledgement="We acknowledge the computer resources and assistance provided by "
                        "Centro de Computación Científica-Universidad Autónoma de Madrid "
                        "(CCC-UAM).",
        docs=("https://ccc.uam.es/en/documentation/FAQ_en.html",),
        verified="guías del CCC-UAM (consultadas 2026-10-17)",
    ),
    "lusitania": ClusterProfile(
        name="lusitania", title="Lusitania", centre="COMPUTAEX (Cáceres)", res_node=True,
        notes=("Sin documentación pública de usuario: pídela a res_support@computaex.es y "
               "usa 'slurmscope probe'.",),
        acknowledgement=RES_ACKNOWLEDGEMENT,
        docs=("https://www.res.es/en/about-res/nodes/lusitania-cenits",),
    ),
    "urederra": ClusterProfile(
        name="urederra", title="Urederra", centre="NASERTIC (Pamplona)", res_node=True,
        notes=("Sin guía pública de usuario: pide información en "
               "https://hpc.nasertic.es/es/solicitud-de-informacion y usa 'slurmscope probe'.",
               "Ficha RES: 180 nodos (Skylake y Broadwell), scratch Lustre, Rocky Linux 9."),
        acknowledgement=RES_ACKNOWLEDGEMENT,
        docs=("https://www.res.es/en/about-res/nodes/urederra-nasertic",),
        verified="ficha del nodo en res.es (consultada 2026-10-17)",
    ),
    "xula": ClusterProfile(
        name="xula", title="Xula", centre="CIEMAT (Madrid)", res_node=True,
        notes=("Sin guía pública de usuario: soporte en res_support@ciemat.es; usa "
               "'slurmscope probe'.",
               "Ficha RES: 144 nodos Xeon Platinum 8160 (48 cores, 96 GB), Lustre, Rocky 8."),
        acknowledgement=RES_ACKNOWLEDGEMENT,
        docs=("https://www.res.es/en/about-res/nodes/xula-turgalium-ciemat",),
        verified="ficha del nodo en res.es (consultada 2026-10-17)",
    ),
    "turgalium": ClusterProfile(
        name="turgalium", title="Turgalium", centre="CETA-CIEMAT (Trujillo)", res_node=True,
        notes=("Sin guía pública de usuario: soporte en res_support@ceta-ciemat.es; usa "
               "'slurmscope probe'.",
               "Ficha RES: 144 nodos Xeon Platinum 8160 (48 cores, 96 GB), Lustre, Rocky 8."),
        acknowledgement=RES_ACKNOWLEDGEMENT,
        docs=("https://www.res.es/en/about-res/nodes/xula-turgalium-ciemat",),
        verified="ficha del nodo en res.es (consultada 2026-10-17)",
    ),
    "dipc-hyperion": ClusterProfile(
        name="dipc-hyperion", title="Hyperion", centre="DIPC (San Sebastián)",
        logins=("hyperion.sw.ehu.es",),
        access="SSH con clave registrada en account-setup.sw.ehu.es.",
        qos_limits={"regular": L("1-00:00:00", 180), "test": L("00:10:00", 2, 2),
                    "long": L("2-00:00:00", 40), "xlong": L("8-00:00:00", 20, 200),
                    "serial": L("2-00:00:00", 1000, 2000)},
        max_submit_total=1000, home_for_jobs=False, workdir_hint="/scratch",
        notes=("Máximo 1000 trabajos en cola por usuario sumando todas las QoS.",
               "Un proceso intensivo en el login suspende la cuenta."),
        docs=("https://scc.dipc.org/docs/jobs/slurm",),
        verified="documentación del DIPC (consultada 2026-10-17)",
    ),
    "generic": ClusterProfile(
        name="generic", title="SLURM genérico", centre="cualquier clúster SLURM",
        notes=("Sin reglas propias: usa 'slurmscope probe' para leer los límites.",),
    ),
}
