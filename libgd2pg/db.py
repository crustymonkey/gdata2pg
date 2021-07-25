import psycopg2
import logging
import time
from datetime import datetime, timedelta
from textwrap import dedent
from typing import Sequence, Dict, TYPE_CHECKING, Optional, Any, List, Tuple


if TYPE_CHECKING:
    from .config import GDConfig


class DB:
    DT_TF = '%Y-%m-%d %H:%M:%S'

    def __init__(self, config: 'GDConfig'):
        self.config = config
        self.conn = self._get_conn()
        self.conn.autocommit = False

    def __del__(self):
        if hasattr(self, 'conn') and self.conn:
            try:
                self.conn.close()
            except Exception:
                pass

    def query(
            self,
            query: str,
            args: Tuple[str]=None,
            dry_run: Optional[bool]=False) -> bool:
        ret = False
        if args is None:
            args = tuple()

        if dry_run:
            logging.info(f'Would have run: {query} with {args}')
            return True


        logging.debug(f'Running arbitrary query: {query}')
        try:
            with self.conn.cursor() as curs:
                curs.execute(query, args)
        except psycopg2.errors.AdminShutdown:
            logging.error('The connection has been terminated, reconnecting')
            self.conn = self._get_conn()
            return self.query(query)
        except Exception as e:
            # Log the exception and roll back
            logging.exception('Failed to insert metrics into the db')
            self.conn.rollback()
        else:
            self.conn.commit()
            ret = True

        logging.debug('Arbitrary query finished')

        return ret

    def insert_metrics(
            self,
            metrics: Dict[str, Dict[str, Any]],
            dt: Optional[datetime]=None,
            minute_mark: Optional[bool]=True) -> bool:
        """
        This will insert the metrics for the specified timestamp.  If
        timestamp is not specified, the current timestamp will be used.
        """
        if not metrics:
            # If we receive an empty set of metrics, return True
            return True

        ret = False
        dt = datetime.utcnow() if dt is None else dt
        if minute_mark:
            # Most recent minute mark
            dt_str = dt.strftime('%Y-%m-%d %H:%M:00')
        else:
            dt_str = dt.strftime('%Y-%m-%d %H:%M:%S')

        query = dedent(
            '''
            INSERT INTO tsd (entity_id, key_id, added, value) VALUES
            (ent_id(%s), key_id(%s), %s, %s);
            ''').strip()

        logging.debug('Starting INSERT query')
        start = time.time()
        try:
            with self.conn.cursor() as curs:
                for entity, keys in metrics.items():
                    for key, val in keys.items():
                        if val is not None:
                            logging.debug(f'Running insert with {entity} '
                                f'{key} {dt_str} {val}')
                            curs.execute(query, (entity, key, dt_str, val))
                        else:
                            logging.warn(
                                f'Found an invalid value for {entity}::{key}, '
                                'not inserting into db'
                            )
        except psycopg2.errors.AdminShutdown:
            logging.error('The connection has been terminated, reconnecting')
            self.conn = self._get_conn()
        except psycopg2.InterfaceError as e:
            logging.error(f'Interface error, reconnecting: {e}')
            self.conn = self._get_conn()
        except Exception as e:
            # Log the exception and roll back
            logging.exception(f'Failed to insert metrics into the db: {e}')
            self.conn.rollback()
        else:
            self.conn.commit()
            ret = True

        itime = time.time() - start
        logging.debug(f'INSERT query finished in {itime:.02f}')

        return ret

    def vacuum(
            self,
            table: Optional[str]='',
            dry_run: Optional[bool]=False,
            full: Optional[bool]=False) -> bool:
        """
        Do a cleanup of the db to reclaim space, optionally supplying a table
        """
        ret = True
        query = 'VACUUM {}ANALYZE'.format('FULL ' if full else '')
        if table:
            query = f'{query} {table}'

        if dry_run:
            logging.info(f'Would have run: {query}')
            return ret

        cur_isolation = self.conn.isolation_level

        try:
            self.conn.set_isolation_level(0)
            curs = self.conn.cursor()
            curs.execute(query)
        except psycopg2.errors.AdminShutdown:
            logging.error('The connection has been terminated, reconnecting')
            self.conn = self._get_conn()
            ret = False
        except Exception as e:
            # Log the exception and roll back
            logging.exception('Failed to vacuum table: {}'.format(table))
            ret = False
        finally:
            self.conn.set_isolation_level(cur_isolation)

        return ret

    def do_rollup(
            self,
            start_time: datetime,  # Most recent time
            roll_period: int,
            end_time: Optional[datetime]=None,  # Further back in time
            dry_run: Optional[bool]=False,  # Rollback the changes
            ):
        """
        This will do a rollup for a time period and an aggregation period
        """
        # Go all the way back if nothing is specified for end time
        end_time = datetime(1970, 1, 1) if not end_time else end_time
        entity_ids = self._get_entities()

        total_rollups = 0
        count = 0
        for eid in entity_ids:
            logging.debug('Running rollups for ent id: {}'.format(eid))
            key_ids = self._get_keys_for_ent(eid)
            if not total_rollups:
                total_rollups = len(entity_ids) * len(key_ids)
            for kid in key_ids:
                count += 1
                perc = (count / total_rollups) * 100
                logging.debug(
                    'Running rollups for key id: {}; {:.01f}% complete'.format(
                        kid,
                        perc,
                    )
                )
                self._rollup_and_del(
                    start_time, roll_period, eid, kid, end_time, dry_run)

    def mv_table_to_tblspace(self, table, tablespace, dry_run=False):
        """
        Move the specified table to a different tablespace
        """
        mv_query = f'ALTER TABLE {table} SET TABLESPACE {tablespace}'
        try:
            with self.conn.cursor() as curs:
                curs.execute(mv_query)
                if dry_run:
                    self.conn.rollback()
        except psycopg2.errors.AdminShutdown:
            logging.error('The connection has been terminated, reconnecting')
            self.conn = self._get_conn()
        except Exception as e:
            # Log the exception and roll back
            logging.exception(f'Failed to move {table} to {tablespace}: {e}')
            self.conn.rollback()
        else:
            self.conn.commit()

    def _rollup_and_del(
            self,
            start_time: datetime,
            roll_period: int,
            ent_id: int,
            key_id: int,
            end_time: datetime,
            dry_run: bool):
        sel_query = dedent(
            '''
            SELECT t.id, t.added, t.value
            FROM tsd t
            WHERE
                entity_id = %s
                AND key_id = %s
                AND added > %s
                AND added < %s
            ORDER BY added
            '''
        )

        ins_query = dedent(
            '''
            INSERT INTO tsd (entity_id, key_id, added, value)
            VALUES (%s, %s, %s, %s)
            '''
        )

        del_query = 'DELETE FROM tsd WHERE id in ({})'

        stime = start_time.strftime('%Y-%m-%d %H:%M:%S')
        etime = end_time.strftime('%Y-%m-%d %H:%M:%S')

        # First, get all the items we need to work on
        with self.conn.cursor() as curs:
            curs.execute(sel_query, (ent_id, key_id, etime, stime))
            to_compress = curs.fetchall()
        self.conn.commit()

        if not to_compress:
            # If we have no metrics for the period, return
            return

        new_vals = self._compress_vals(to_compress, roll_period)
        # modify new vals for db insertion
        new_vals = [(ent_id, key_id, d, v) for d, v in new_vals]

        # We'll do all this in a transaction
        try:
            with self.conn.cursor() as curs:
                # First we'll delete
                curs.execute(del_query.format(
                    ', '.join([str(d[0]) for d in to_compress])))
                # Now we add the new items
                curs.executemany(ins_query, new_vals)
                if dry_run:
                    self.conn.rollback()
        except psycopg2.errors.AdminShutdown:
            logging.error('The connection has been terminated, reconnecting')
            self.conn = self._get_conn()
        except Exception as e:
            # Log the exception and roll back
            logging.exception('Failed to insert metrics into the db')
            self.conn.rollback()
        else:
            self.conn.commit()

    def _compress_vals(
            self,
            to_compress: List[Tuple[int, str, float]],
            roll_period: int) -> List[Tuple[str, float]]:
        td = timedelta(seconds=roll_period)
        ret = []

        cur_dt = to_compress[0][1]
        last_add = to_compress[0][1]
        cur_vals = []
        for row in to_compress:
            _, added, val = row

            if added - td >= cur_dt:
                # Need to rollup the vals and reset everything
                new_val = sum(cur_vals) / len(cur_vals)
                ret.append((last_add, new_val))

                cur_dt = added
                cur_vals = [val]
            else:
                cur_vals.append(val)

            last_add = added

        if cur_vals:
            # Add the remainder
            new_val = sum(cur_vals) / len(cur_vals)
            ret.append((last_add, new_val))

        return ret

    def _get_entities(self) -> List[int]:
        query = 'SELECT id FROM entities'
        with self.conn.cursor() as curs:
            curs.execute(query)
            ret = curs.fetchall()

        return [e[0] for e in ret]

    def _get_keys_for_ent(self, ent: int) -> List[int]:
        query = dedent(
            '''
            SELECT DISTINCT k.id from keys k, entities e, tsd t
            WHERE
                e.id = %s
                AND e.id = t.entity_id
                AND k.id = t.key_id
            '''
        )
        with self.conn.cursor() as curs:
            curs.execute(query, (ent,))
            ret = curs.fetchall()

        return [k[0] for k in ret]

    def _get_conn(self) -> psycopg2.extensions.connection:
        """
        Returns the database connection
        """
        # TODO: Add generic support for MySQL instead of hard coding postgres
        # only
        host, port = self.config['main']['db_loc'].split(':')
        port = int(port)

        conn = psycopg2.connect(
            dbname=self.config['main']['db_name'],
            user=self.config['main']['db_user'],
            password=self.config['main']['db_password'],
            host=host,
            port=port,
        )

        return conn

