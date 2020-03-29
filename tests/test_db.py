from datetime import datetime as dt
from libgd2pg.config import GDConfig
from unittest.mock import MagicMock, patch
from libgd2pg.db import DB
import os
import unittest

CONF_FILE = os.path.join(
    os.path.dirname(__file__),
    '..',
    'gdata2pg.ini.default',
)

class TestDB(unittest.TestCase):
    def setUp(self):
        self.config = GDConfig()
        self.config.read(CONF_FILE)
        mock = MagicMock(return_value=MagicMock())
        with patch.object(DB, '_get_conn', mock):
            self.db = DB(self.config)

    def test_compress(self):
        test_data = [
            (1, dt.strptime('2020-03-20 10:00:00', self.db.DT_TF), 1),
            (1, dt.strptime('2020-03-20 10:01:00', self.db.DT_TF), 2),
            (1, dt.strptime('2020-03-20 10:02:00', self.db.DT_TF), 3),
            (1, dt.strptime('2020-03-20 10:03:00', self.db.DT_TF), 4),
            (1, dt.strptime('2020-03-20 10:04:00', self.db.DT_TF), 5),

            (1, dt.strptime('2020-03-20 10:05:00', self.db.DT_TF), 1),
            (1, dt.strptime('2020-03-20 10:06:00', self.db.DT_TF), 2),
            (1, dt.strptime('2020-03-20 10:07:00', self.db.DT_TF), 3),
            (1, dt.strptime('2020-03-20 10:08:00', self.db.DT_TF), 4),
            (1, dt.strptime('2020-03-20 10:09:00', self.db.DT_TF), 5),

            (1, dt.strptime('2020-03-20 10:10:00', self.db.DT_TF), 1),
        ]
        expected = [
            (dt.strptime('2020-03-20 10:04:00', self.db.DT_TF), 3.0),
            (dt.strptime('2020-03-20 10:09:00', self.db.DT_TF), 3.0),
            (dt.strptime('2020-03-20 10:10:00', self.db.DT_TF), 1),
        ]

        ret = self.db._compress_vals(test_data, 300)

        self.assertEqual(ret, expected)
