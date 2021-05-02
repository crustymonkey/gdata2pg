import logging
import time
from threading import Thread, Timer, Event
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .datamanager import DataManager
    from .db import DB

class InsTimer(Thread):
    """
    This just manages the data collection and db insert functionality at
    the given intervals
    """
    
    def __init__(
            self,
            dm: 'DataManager',
            db: 'DB',
            name: Optional[str]='InsTime'):
        super().__init__(name=name)
        self.dm = dm
        self.db = db
        self.daemon = True
        self._stop_ev = Event()

    def run(self) -> None:
        logging.debug('Timer thread started')
        if not self._stop_ev.is_set():
            self._start_timer_and_do_work()

    def stop(self) -> None:
        logging.info('Setting the stop event in the timer')
        self._stop_ev.set()

    def _start_timer_and_do_work(self) -> None:
        logging.debug('Starting timer')
        Timer(self._get_next_min_diff(), self._do_work).start()

    def _do_work(self) -> None:
        logging.debug('Doing work')
        metrics = None
        res = None
        try:
            metrics = self.dm.get_metrics_reset()
        except Exception as e:
            logging.exception('Error getting metrics')

        if metrics:
            try:
                res = self.db.insert_metrics(metrics)
            except Exception as e:
                logging.exception('Error inserting metrics into db')

        logging.debug(f'Insert result: {res}')

        if self._stop_ev.is_set():
            logging.info('Work completed, closing thread')
        else:
            self._start_timer_and_do_work()

    def _get_next_min_diff(self) -> float:
        """
        returns how many seconds until the next minute boundary
        """
        now = time.time()
        ret = 60 - now % 60
        logging.debug(f'Time to next minute: {ret:.02f}')

        return ret
    
