#!/usr/bin/env python3

import logging
import sys
from argparse import ArgumentParser
from datetime import datetime, timedelta
from libgd2pg.db import DB
from libgd2pg.config import GDConfig


def get_args():
    p = ArgumentParser(description='Perform the data rollups')
    p.add_argument('-c', '--config', default='/etc/gdata2pg/gdata2pg.ini',
        help='The path to the config file [default: %(default)s]')
    p.add_argument('-D', '--debug', action='store_true', default=False,
        help='Add debug output [default: %(default)s]')

    args = p.parse_args()

    return args


def setup_logging(args):
    level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=level,
    )


def main():
    args = get_args()
    setup_logging(args)
    conf = GDConfig()
    conf.read(args.config)

    db = DB(conf)

    start = datetime.strptime('2020-02-19 12:20:46', db.DT_TF)
    end = datetime.strptime('2020-02-19 12:00:46', db.DT_TF)
    db._rollup_and_del(start, 300, 1, 1, end)

    return 0


if __name__ == '__main__':
    sys.exit(main())
