import logging
import time
from threading import Thread, Timer, Event
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .datamanager import DataManager
    from .db import DB

class InsTimer(Thread):
    """
    This just manages the data collection and db insert functionality at
    the given intervals
    """
    
    def __init__(self, dm: 'DataManager', db: 'DB'):
        self.dm = dm
        self.db = db
        self.daemon = True
        self._stop = Event()

    def run(self) -> None:
        logging.debug('Timer thread started')
        if not self._stop.is_set():
            self._start_timer_and_do_work()

    def stop(self) -> None:
        logging.info('Setting the stop event in the timer')
        self._stop.set()

    def _start_timer_and_do_work(self) -> None:
        logging.debug('Starting timer')
        Timer(self._get_next_min_diff(), self._do_work).start()

    def _do_work(self) -> None:
        logging.debug('Doing work')
        metrics = self.dm.get_metrics_reset()
        logging.debug(f'Got metrics: {metrics}')
        res = self.db.insert_metrics(metrics)
        logging.debug(f'Insert result: {res}')

        if not self._stop.is_set():
            logging.debug('Starting the timer again')
            self._start_timer_and_do_work()

        logging.info('Work completed, closing thread')

    def _get_next_min_diff(self) -> float:
        """
        returns how many seconds until the next minute boundary
        """
        now = time.time()
        ret = 60 - now % 60
        logging.debug(f'Time to next minute: {ret:.02f}')

        return ret
    
