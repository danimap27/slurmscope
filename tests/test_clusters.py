"""Perfiles de clúster, troceo de job arrays y sondeo del clúster."""

import unittest

from slurmscope.clusters import (
    PROFILES,
    SLURM_DEFAULT_MAX_ARRAY_SIZE,
    ClusterConfig,
    ClusterConfigError,
    check_submission_rules,
    get_profile,
    parse_probe_output,
    plan_chunks,
    probe_cluster,
)

# Salida real de Hércules (17-10-2026), recortada a lo que se interpreta.
HERCULES_PROBE = """##version
slurm 22.05.2
##config
MaxArraySize            = 500
MaxJobCount             = 10000
##partitions
standard*|up|7-00:00:00|126/5/95/226
gpu|up|7-00:00:00|2/1/3/6
##assoc
hercules|||normal
hercules||normal
##python
Python 3.6.8
##end
"""


class TestPerfiles(unittest.TestCase):
    def test_perfiles_incluidos(self):
        for name in ("hercules", "marenostrum5-gpp", "marenostrum5-acc",
                     "finisterrae3", "tirant", "picasso", "calendula", "generic"):
            self.assertIn(name, PROFILES)

    def test_hercules_comprobado(self):
        profile = get_profile("hercules")
        self.assertEqual(profile.max_array_size, 500)
        self.assertEqual(profile.partition, "standard")
        self.assertTrue(profile.verified)

    def test_marenostrum_exige_cuenta_y_qos(self):
        profile = get_profile("marenostrum5-gpp")
        problems = check_submission_rules(profile, None, None)
        self.assertEqual(len(problems), 2)
        self.assertEqual(check_submission_rules(profile, "upo99", "gp_resb"), [])

    def test_perfil_desconocido(self):
        with self.assertRaises(ClusterConfigError):
            get_profile("cray-imaginario")

    def test_mayusculas_y_espacios(self):
        self.assertEqual(get_profile("  Hercules ").name, "hercules")


class TestClusterConfig(unittest.TestCase):
    def test_por_defecto_es_local(self):
        cfg = ClusterConfig.from_mapping(None)
        self.assertFalse(cfg.is_remote)
        self.assertEqual(cfg.effective_max_array_size(), SLURM_DEFAULT_MAX_ARRAY_SIZE)

    def test_max_array_size_del_perfil(self):
        cfg = ClusterConfig.from_mapping({"profile": "hercules"})
        self.assertEqual(cfg.effective_max_array_size(), 500)

    def test_la_especificacion_manda_sobre_el_perfil(self):
        cfg = ClusterConfig.from_mapping({"profile": "hercules", "max_array_size": 100})
        self.assertEqual(cfg.effective_max_array_size(), 100)

    def test_claves_desconocidas(self):
        with self.assertRaises(ClusterConfigError):
            ClusterConfig.from_mapping({"hots": "x"})

    def test_host_que_parece_una_opcion_es_rechazado(self):
        with self.assertRaises(ClusterConfigError):
            ClusterConfig.from_mapping({"host": "-oProxyCommand=evil"})

    def test_remote_dir(self):
        cfg = ClusterConfig.from_mapping({"host": "h"})
        self.assertEqual(cfg.resolved_remote_dir("qtl"), "slurmscope/qtl")
        cfg = ClusterConfig.from_mapping({"host": "h", "remote_dir": "exp/{project}"})
        self.assertEqual(cfg.resolved_remote_dir("qtl"), "exp/qtl")

    def test_remote_dir_peligroso(self):
        for bad in ("~", "/", "../fuera", "a/../../b"):
            cfg = ClusterConfig.from_mapping({"host": "h", "remote_dir": bad})
            with self.assertRaises(ClusterConfigError, msg=bad):
                cfg.resolved_remote_dir("p")

    def test_max_array_size_invalido(self):
        for bad in ("muchos", 1, 0):
            with self.assertRaises(ClusterConfigError):
                ClusterConfig.from_mapping({"max_array_size": bad})


class TestTroceo(unittest.TestCase):
    def test_cabe_en_un_array(self):
        chunks = plan_chunks(80, 500)
        self.assertEqual(len(chunks), 1)
        self.assertEqual((chunks[0].offset, chunks[0].size), (0, 80))

    def test_hercules_1200_ejecuciones(self):
        # MaxArraySize=500 → índices 0..499 → como mucho 499 tareas por array
        chunks = plan_chunks(1200, 500)
        self.assertEqual([c.size for c in chunks], [499, 499, 202])
        self.assertEqual([c.offset for c in chunks], [0, 499, 998])
        self.assertEqual(chunks[-1].last_run, 1200)

    def test_cubre_todo_sin_solapes(self):
        for n in (1, 498, 499, 500, 998, 999, 1000, 4321):
            chunks = plan_chunks(n, 500)
            covered = [i for c in chunks for i in range(c.first_run, c.last_run + 1)]
            self.assertEqual(covered, list(range(1, n + 1)), n)
            self.assertTrue(all(c.size <= 499 for c in chunks))

    def test_array_spec(self):
        chunk = plan_chunks(1200, 500)[1]
        self.assertEqual(chunk.array_spec(40), "1-499%40")
        self.assertEqual(chunk.array_spec(0), "1-499")

    def test_cero_ejecuciones(self):
        self.assertEqual(plan_chunks(0, 500), [])


class TestSondeo(unittest.TestCase):
    def test_salida_real_de_hercules(self):
        result = parse_probe_output(HERCULES_PROBE)
        self.assertEqual(result.slurm_version, "22.05.2")
        self.assertEqual(result.max_array_size, 500)
        self.assertEqual(result.max_job_count, 10000)
        self.assertEqual(result.python, "3.6.8")
        names = [p["name"] for p in result.partitions]
        self.assertEqual(names, ["standard", "gpu"])
        self.assertEqual(result.partitions[0]["default"], "sí")
        self.assertEqual(result.partitions[0]["total_nodes"], "226")
        self.assertEqual(result.warnings, [])

    def test_salida_incompleta_avisa(self):
        result = parse_probe_output("##version\nbash: sinfo: command not found\n##end\n")
        self.assertIsNone(result.max_array_size)
        self.assertTrue(result.warnings)

    def test_un_solo_viaje(self):
        calls = []

        def runner(argv):
            calls.append(argv)
            return HERCULES_PROBE

        result = probe_cluster(runner)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][:2], ["bash", "-lc"])
        self.assertEqual(result.max_array_size, 500)


if __name__ == "__main__":
    unittest.main()
