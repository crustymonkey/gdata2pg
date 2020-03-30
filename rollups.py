#!/usr/bin/env python3

import logging
import sys
from argparse import ArgumentParser
from dateparser import parse as dparse
from datetime import datetime, timedelta
from libgd2pg.db import DB
from libgd2pg.config import GDConfig


def get_args():
    p = ArgumentParser(description='Perform the data rollups')
    p.add_argument('-c', '--config', default='/etc/gdata2pg/gdata2pg.ini',
        help='The path to the config file [default: %(default)s]')
    p.add_argument('-d', '--dry-run', default=False, action='store_true',
        help='Don\'t actually commit the changes (roll back the trans.) '
        '[default: %(default)s]')
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


def do_rollups(db, conf, args):
    for roll in conf['rollups'].getlist('rollups'):
        start = dparse(conf[roll]['start_time'])
        end = conf[roll]['end_time']
        if end:
            end = dparse(end)
        period = conf[roll].getint('rollup_period')
        logging.debug('Running rollup for {}'.format(roll))
        db.do_rollup(start, period, end, args.dry_run)


def main():
    args = get_args()
    setup_logging(args)
    conf = GDConfig()
    conf.read(args.config)

    db = DB(conf)
    do_rollups(db, conf, args)

    # Now, try and cleanup disk space
    logging.debug('Vacuuming DB table: tsd')
    db.vacuum('tsd')

    return 0


if __name__ == '__main__':
    sys.exit(main())
