#!/usr/bin/env python3

import logging
import sys
from argparse import ArgumentParser
from libgd2pg.flask import APP, flask_init


def get_args():
    p = ArgumentParser()
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
    flask_init(args)
    APP.run()

    return 0


if __name__ == '__main__':
    sys.exit(main())
