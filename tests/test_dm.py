from libgd2pg.config import GDConfig
from unittest.mock import MagicMock
import libgd2pg.datamanager as dmgr
import os
import unittest

CONF_FILE = os.path.join(
    os.path.dirname(__file__),
    '..',
    'gdata2pg.ini.default',
)

class TestDM(unittest.TestCase):
    def setUp(self):
        self.config = GDConfig()
        self.config.read(CONF_FILE)
        self._reset_dm()
        self._test_dtups = {
            'd1': [
                dmgr.DataTup('a', 'derive', 1.1),
                dmgr.DataTup('a', 'derive', 3.3),
                dmgr.DataTup('a', 'derive', 4.2),
            ],
            'd2': [
                dmgr.DataTup('b', 'gauge', 1),
                dmgr.DataTup('b', 'gauge', 3),
                dmgr.DataTup('b', 'gauge', 4),
                dmgr.DataTup('b', 'gauge', 9),
            ],
        }

    def test_push(self):
        d1 = {
            'host': 'test_host',
            'plugin': 'plugin',
        }

        d2 = {
            'host': 'test_host',
            'plugin': 'plugin2',
        }
        
        dmg = dmgr.dm()

        dmg.push(d1)

        self.assertEqual(
            dmg.ent_map,
            {d1['host']: [d1]},
        )

        self._reset_dm()

        dmg = dmgr.dm()
        dmg.push([d1, d2])

        self.assertEqual(
            dmg.ent_map,
            {d1['host']: [d1, d2]},
        )

    def test_priv_get_metrics(self):
        d1 = {
            "values": [
                605339,
                247494
            ],
            "dstypes": [
                "derive",
                "derive"
            ],
            "dsnames": [
                "rx",
                "tx"
            ],
            "plugin": "interface",
            "plugin_instance": "enp0s3",
            "type": "if_packets",
            "type_instance": ""
        }

        d2 = {
            "values": [
                482
            ],
            "dstypes": [
                "derive"
            ],
            "dsnames": [
                "value"
            ],
            "plugin": "cpu",
            "plugin_instance": "4",
            "type": "cpu",
            "type_instance": "wait"
        }

        d3 = {
            "values": [
                51167232
            ],
            "dstypes": [
                "gauge"
            ],
            "dsnames": [
                "value"
            ],
            "plugin": "df",
            "plugin_instance": "boot",
            "type": "df_complex",
            "type_instance": "reserved"
        } 

        dmg = dmgr.dm()
        ret = dmg._get_metrics(d1)
        self.assertEqual(
            ret,
            [
                dmgr.DataTup(
                    'interface.enp0s3.if_packets.rx', 'derive', 605339),
                dmgr.DataTup(
                    'interface.enp0s3.if_packets.tx', 'derive', 247494),
            ],
        )
        self._reset_dm()

        dmg = dmgr.dm()
        ret = dmg._get_metrics(d2)
        self.assertEqual(
            ret,
            [
                dmgr.DataTup('cpu.4.wait', 'derive', 482),
            ],
        )
        self._reset_dm()
        dmg = dmgr.dm()
        ret = dmg._get_metrics(d3)
        self.assertEqual(
            ret,
            [
                dmgr.DataTup('df.boot.df_complex.reserved', 'gauge', 51167232),
            ],
        )
        self._reset_dm()

    def test_priv_comp_sum(self):
        dmg = dmgr.dm()

        ret = dmg._comp_sum(self._test_dtups['d1'])
        self.assertAlmostEqual(ret, 8.6)

        ret = dmg._comp_sum(self._test_dtups['d2'])
        self.assertEqual(ret, 17)
        self._reset_dm()

    def test_priv_comp_avg(self):
        dmg = dmgr.dm()

        ret = dmg._comp_avg(self._test_dtups['d1'])
        self.assertAlmostEqual(ret, 2.8667, 4)

        ret = dmg._comp_avg(self._test_dtups['d2'])
        self.assertAlmostEqual(ret, 4.25)
        self._reset_dm()

    def test_priv_comp_pct(self):
        dmg = dmgr.dm()

        for perc, val in (
                (50, 3.3),
                (90, 4.02),
                (95, 4.11),
                (99, 4.182)):
            ret = dmg._comp_pct(self._test_dtups['d1'], perc)
            self.assertAlmostEqual(ret, val)

        for perc, val in (
                (50, 3.5),
                (90, 7.5),
                (95, 8.25),
                (99, 8.85)):
            ret = dmg._comp_pct(self._test_dtups['d2'], perc)
            self.assertAlmostEqual(ret, val)

        self._reset_dm()

    def test_priv_get_comp_metrics(self):
        dmg = dmgr.dm()
        agg_dtups = {
            'a': self._test_dtups['d1'],
            'b': self._test_dtups['d2'],
        }

        ret = dmg._get_comp_metrics(agg_dtups)
        expected = {
            'a.sumb': 3.1,
            'b.avg': 4.25,
            'b.p50': 3.5,
            'b.p90': 7.5,
            'b.p95': 8.25,
            'b.p99': 8.85,
        }
        self.assertListEqual(
            list(ret.keys()),
            list(expected.keys()),
        )

        for k, v in ret.items():
            self.assertAlmostEqual(v, expected[k])

        self._reset_dm()

    def test_priv_get_agg_dtups(self):
        # TODO
        pass

    def _reset_dm(self):
        dmgr.DM = None
        dmgr.dm(self.config)


if __name__ == '__main__':
    unittest.main()
