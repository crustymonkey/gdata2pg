#!/usr/bin/env python3

import logging
import sys
import time
from argparse import ArgumentParser
from dateparser import parse as dparse
from datetime import datetime, timedelta, date
from libgd2pg.db import DB
from libgd2pg.config import GDConfig


PART_TPL = '{table}_{year}{month}'
INDEXES = (
    'CREATE INDEX ON {table} (added)',
    'CREATE INDEX ON {table} (entity_id)',
    'CREATE INDEX ON {table} (key_id)',
)


def get_args():
    p = ArgumentParser(description='Perform the data rollups')
    p.add_argument('-c', '--config', default='/etc/gdata2pg/gdata2pg.ini',
        help='The path to the config file [default: %(default)s]')
    p.add_argument('-p', '--also-partition', default=False, action='store_true',
        help='Also partition the tables [default: %(default)s]')
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


def do_partition(db, conf, args):
    gm = time.gmtime()
    if gm.tm_mday != 21:
        # Only create next month's partition on the 25th of the month
        return

    new_d = date.today() + timedelta(days=32)
    # Create next month's partition
    part = PART_TPL.format(
        table='tsd',
        year=new_d.year,
        month=f'{new_d.month:02d}',
    )
    
    start = f'{new_d.year}-{new_d.month:02d}-01'
    end_d = new_d + timedelta(days=32)
    end = f'{end_d.year}-{end_d.month:02d}-01'

    if db.query(
            f'CREATE TABLE IF NOT EXISTS {part} PARTITION OF tsd '
            'FOR VALUES FROM (%s) to (%s)',
            (start, end),
            args.dry_run):
        for idx in INDEXES:
            index = idx.format(part, dry_run=args.dry_run)
            if not db.query(index):
                logging.error(f'Failed to create index: {index}')
    else:
        logging.error(f'Failed to create partition: {part}')


def main():
    args = get_args()
    setup_logging(args)
    conf = GDConfig()
    conf.read(args.config)

    db = DB(conf)
    do_rollups(db, conf, args)
    
    if args.also_partition:
        do_partition(db, conf, args)

    # Now, try and cleanup disk space
    logging.debug('Vacuuming DB table: tsd')
    db.vacuum('tsd')

    return 0


if __name__ == '__main__':
    sys.exit(main())
