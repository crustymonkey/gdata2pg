#!/usr/bin/env python3

import logging
import sys
from argparse import ArgumentParser


def get_args():
    p = ArgumentParser(description='Perform the data rollups')
    # TODO: make these mutually exclusive
    p.add_argument('-m', '--monthly', action='store_true', default=False,
        help='Perform the monthly rollups [default: %(default)s]')
    p.add_argument('-w', '--weekly', action='store_true', default=False,
        help='Perform the weekly rollups [default: %(default)s]')
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

    return 0


if __name__ == '__main__':
    sys.exit(main())
