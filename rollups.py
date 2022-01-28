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
TSD_INDEXES = (
    'CREATE INDEX IF NOT EXISTS {table}_added_idx ON {table} (added)',
    'CREATE INDEX IF NOT EXISTS {table}_entity_id_idx ON {table} (entity_id)',
    'CREATE INDEX IF NOT EXISTS {table}_key_id_idx ON {table} (key_id)',
    'CREATE INDEX IF NOT EXISTS {table}_id_idx ON {table} (id)',
)
WEBLOGS_INDEXES = (
    'CREATE INDEX IF NOT EXISTS {table}_asn_idx ON {table} (asn)',
    'CREATE INDEX IF NOT EXISTS {table}_aso_idx ON {table} (aso)',
    'CREATE INDEX IF NOT EXISTS {table}_city_idx ON {table} (city)',
    'CREATE INDEX IF NOT EXISTS {table}_country_idx ON {table} (country)',
    'CREATE INDEX IF NOT EXISTS {table}_country_iso_code_idx ON {table} (country_iso_code)',
    'CREATE INDEX IF NOT EXISTS {table}_dt_idx ON {table} (dt)',
    'CREATE INDEX IF NOT EXISTS {table}_host_idx ON {table} (host)',
    'CREATE INDEX IF NOT EXISTS {table}_id_idx ON {table} (id)',
    'CREATE INDEX IF NOT EXISTS {table}_ip_idx ON {table} (ip)',
    'CREATE INDEX IF NOT EXISTS {table}_network_idx ON {table} (network)',
    'CREATE INDEX IF NOT EXISTS {table}_referer_idx ON {table} (referer)',
    'CREATE INDEX IF NOT EXISTS {table}_req_http_vers_idx ON {table} (req_http_vers)',
    'CREATE INDEX IF NOT EXISTS {table}_req_type_idx ON {table} (req_type)',
    'CREATE INDEX IF NOT EXISTS {table}_req_uri_idx ON {table} (req_uri)',
    'CREATE INDEX IF NOT EXISTS {table}_request_idx ON {table} (request)',
    'CREATE INDEX IF NOT EXISTS {table}_site_idx ON {table} (site)',
    'CREATE INDEX IF NOT EXISTS {table}_state_idx ON {table} (state)',
    'CREATE INDEX IF NOT EXISTS {table}_status_idx ON {table} (status)',
    'CREATE INDEX IF NOT EXISTS {table}_ua_idx ON {table} (ua)',
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
    p.add_argument('-t', '--also-tablespace', default=False,
        action='store_true', help='Also move items between tablespaces '
        '[default: %(default)s]')
    p.add_argument('-f', '--vacuum-full', default=False, action='store_true',
        help='Do a full vacuum after rollups [default: %(default)s]')
    p.add_argument('-v', '--vacuum-time', default='9:00:00',
        help='Sleep until this time to run the vacuum [default: %(default)s]')
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


def do_partition(base_tbl, db, conf, args):
    gm = time.gmtime()
    if gm.tm_mday > 7:
        # Only create next month's partition during the first week of a
        # new month
        logging.debug(
            'New partitions are only created during the first '
            'week of a month'
        )
        return

    new_d = date.today() + timedelta(days=32)
    # Create next month's partition
    part = PART_TPL.format(
        table=base_tbl,
        year=new_d.year,
        month=f'{new_d.month:02d}',
    )

    start = f'{new_d.year}-{new_d.month:02d}-01'
    end_d = new_d + timedelta(days=32)
    end = f'{end_d.year}-{end_d.month:02d}-01'
    indexes = TSD_INDEXES if base_tbl == 'tsd' else WEBLOGS_INDEXES

    logging.debug(f'Creating a new partition for "{base_tbl}" -> {part}')
    if db.query(
            f'CREATE TABLE IF NOT EXISTS {part} PARTITION OF {base_tbl} '
            'FOR VALUES FROM (%s) to (%s)',
            (start, end),
            args.dry_run):
        for idx in indexes:
            index = idx.format(table=part)
            if not db.query(index, dry_run=args.dry_run):
                logging.error(f'Failed to create index: {index}')
    else:
        logging.error(f'Failed to create partition: {part}')


def get_prev_m() -> datetime:
    """
    Return a datetime object for now minus 30 days
    """
    return datetime.now() - timedelta(days=30)


def do_tablespace_rollup(db, conf, args):
    dest_tblspc = conf.get('tablespace', 'dest_tblspc')
    patterns = conf.getlist('tablespace', 'move_patterns')

    prev_m = get_prev_m()
    year = prev_m.year
    month = f'{prev_m.month:02d}'
    for pattern in patterns:
        tbl = pattern.format(year=year, month=month)
        logging.debug(f'Moving {tbl} to {dest_tblspc}')
        db.mv_table_to_tblspace(tbl, dest_tblspc, args.dry_run)


def parse_time(t):
    """
    parse a time string in the format of "H:M:S" and return a tuple of
    ints in the same order
    """
    return tuple([int(i) for i in t.split(':')])


def run_vacuum(db, args):
    # Need to get the time differential for the sleep
    now = datetime.now()
    hour, minute, sec = parse_time(args.vacuum_time)
    run_time = now.replace(hour=hour, minute=minute, second=sec)

    if run_time - now < timedelta(seconds=2):
        # We have a time in the past, add 1 day
        run_time += timedelta(days=1)

    delta = run_time - now
    delta_secs = delta.total_seconds()

    logging.info(f'Sleeping for {delta_secs} before running VACUUM')
    time.sleep(delta_secs)

    db.vacuum(dry_run=args.dry_run, full=args.vacuum_full)


def main():
    args = get_args()
    setup_logging(args)
    conf = GDConfig()
    conf.read(args.config)

    db = DB(conf)
    if args.also_tablespace:
        do_tablespace_rollup(db, conf, args)

    do_rollups(db, conf, args)

    if args.also_partition:
        do_partition('tsd', db, conf, args)
        do_partition('weblogs', db, conf, args)

    # Now, try and cleanup disk space
    run_vacuum(db, args)

    return 0


if __name__ == '__main__':
    sys.exit(main())
