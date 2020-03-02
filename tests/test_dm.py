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

    def _reset_dm(self):
        dmgr.DM = None
        dmgr.dm(self.config)


if __name__ == '__main__':
    unittest.main()
