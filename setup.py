#!/usr/bin/env python3

from setuptools import setup
import os

def get_requirements():
    cur_dir = os.path.dirname(__file__)
    with open(os.path.join(cur_dir, 'requirements.txt')) as fh:
        return [s.strip() for s in fh.readlines() if s.strip()]

setup(
    name='gdata2pg',
    version='0.1.4',
    author='Jay Deiman',
    author_email='admin@splitstreams.com',
    description=(
        'A data receiver for collectd that inserts data into a Postgres '
        'database for use with Grafana'
    ),
    license='GPLv2',
    keywords='grafana http postgresql',
    packages=['libgd2pg'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License v2 (GPLv2)',
        'Programming Language :: Python :: 3',
        'Topic :: System :: Monitoring',
    ],
    scripts=['server.py', 'rollups.py'],
    include_package_data=True,
    package_data={'': ['etc/gdata2pg.ini.default', 'tsd.sql']},
)
