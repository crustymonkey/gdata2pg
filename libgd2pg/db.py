import psycopg2
import logging
from datetime import datetime
from textwrap import dedent
from typing import Sequence, Dict, TYPE_CHECKING, Optional, Any


if TYPE_CHECKING:
    from .config import GDConfig


class DB:
    def __init__(self, config: 'GDConfig'):
        self.config = config
        self.conn = self._get_conn()
        self.conn.autocommit = False

    def __del__(self):
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass

    def insert_metrics(
            self,
            metrics: Dict[str, Dict[str, Any]],
            dt: Optional[datetime]=None,
            minute_mark: Optional[bool]=True,
            ) -> bool:
        """
        This will insert the metrics for the specified timestamp.  If
        timestamp is not specified, the current timestamp will be used.
        """
        if not metrics:
            # If we receive an empty set of metrics, return True
            return True

        ret = False
        dt = datetime.now() if dt is None else dt
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

        try:
            with self.conn as curs:
                for entity, keys in metrics.items():
                    for key, val in keys.items():
                        curs.execute(query, (entity, key, dt_str, val))
        except Exception as e:
            # Log the exception and roll back
            logging.exception('Failed to insert metrics into the db')
            self.conn.rollback()
        else:
            self.conn.commit()
            ret = True

        return ret

    def _get_conn(self) -> psycopg2.connection:
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

