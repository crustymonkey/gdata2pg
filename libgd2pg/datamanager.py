from .error import InvalidConfigError, InvalidDataError
from .config import GDConfig
from collections import defaultdict, namedtuple
from io import StringIO
from numpy import percentile
from threading import RLock
from typing import (
    Union,
    Dict,
    Sequence,
    DefaultDict,
    List,
    Any,
    Tuple,
    NamedTuple,
)
import logging
import re


class DataTup(NamedTuple):
    """
    This is an intermediate data tuple used in _get_metric
    """
    name: str
    type: str
    value: Union[int, float]


class DataManager:
    PCT_RE = re.compile('pct\((\d+)\)', re.I)
    LOCK = RLock()

    def __init__(self, config: GDConfig):
        self.config = config
        self.ent_map = self._init_map()  # Map entities to received data

    def push(self, data: Union[Dict, Sequence[Dict]]) -> None:
        """
        This will push data into the ent map.  This takes either a list
        or a single dict of data
        """
        if isinstance(data, dict):
            data = [data]

        # Loop over all the data dicts, and manage those
        for d in data:
            try:
                ent = d['host']
            except Exception:
                logging.error('Invalid data passed in, missing "host"')
                logging.debug('DATA: {}'.format(d))
                continue

            self.ent_map[ent].append(d)

    def get_metrics(self) -> Dict[str, Dict[str, Any]]:
        """
        This will roll up and return the aggregated metrics
        """
        ret = {}
        logging.debug('get_metrics() call started')
        self.LOCK.acquire()
        for ent, data in self.ent_map.items():
            try:
                # First we have the get the "compiled" metric name/type/value
                # DataTups to create an intermediate dictionary that we can use
                # to compute the aggregated data points
                agg_dtups = self._get_agg_dtups(data)
                
                # Now we need to get the computed metrics for the data
                comp_metrics = self._get_comp_metrics(agg_dtups)

                # And finally, we attach that dictionary to our ent
                ret[ent] = comp_metrics
            except Exception as e:
                logging.error(f'Failure in get_metrics in the datamanager: {e}')
        self.LOCK.release()
        logging.debug('get_metrics() call finished')

        return ret

    def get_metrics_reset(self) -> Dict[str, Dict[str, Any]]:
        """
        This will get the metrics and reset the internal ent map
        """
        metrics = None
        logging.debug('get_metrics_reset() call started')
        self.LOCK.acquire()
        try:
            metrics = self.get_metrics()
            self.ent_map = self._init_map()
        except Exception:
            logging.exception('Failed to get metrics')
        finally:
            self.LOCK.release()
        logging.debug('get_metrics_reset() call finished')

        return metrics

    def _init_map(self) -> DefaultDict[str, List[Dict[str, Any]]]:
        return defaultdict(list)

    def _get_metrics(self, data: Dict[str, Any]) -> List[DataTup]:
        """
        Calculates the metric name from the data in the dict and returns
        a list of DataTup for each specific metric in this data instance
        """
        ret = []
        s = StringIO()
        try:
            s.write(data['plugin'])

            if data['plugin_instance']:
                s.write('.{}'.format(data['plugin_instance']))

            if data['plugin'] != data['type']:
                s.write('.{}'.format(data['type']))

            if data['type_instance']:
                s.write('.{}'.format(data['type_instance']))

            metric_name = s.getvalue()
            for i, dsn in enumerate(data['dsnames']):
                if dsn == 'value':
                    ret.append(DataTup(
                        metric_name, data['dstypes'][i], data['values'][i]))
                else:
                    tmp = '{}.{}'.format(metric_name, dsn)
                    if data['values'][i] is not None:
                        ret.append(DataTup(
                            tmp, data['dstypes'][i], data['values'][i]))
        except Exception:
            logging.error('Invalid data object for metric name')
            logging.debug('DATA: {}'.format(data))
            raise InvalidDataError('Invalid data: {}'.format(data))

        # If we've made it here, return the metrics
        return ret

    def _get_agg_dtups(
            self,
            data: List[Dict[str, Any]],
            ) -> Dict[str, List[DataTup]]:
        """
        This will return a mapping of metric name -> list of DataTups
        """
        metrics = defaultdict(list)
        for datad in data:
            dtups = self._get_metrics(datad)
            for dtup in dtups:
                metrics[dtup.name].append(dtup)

        return metrics

    def _get_comp_metrics(
            self,
            agg_dtups: Dict[str, List[DataTup]],
            ) -> Dict[str, Any]:
        """
        Given the dictionary including the list of aggregated DataTups,
        return a final dict of computed metric_name -> value
        """
        ret = {}
        for metric_name, data in agg_dtups.items():
            # TODO: Add rollup overrides on metric name
            rollups = self.config['main'].getlist(
                'rollups_{}'.format(data[0].type))
            for rollup in rollups:
                # Get the computed result for the rollup
                suffix = rollup
                m = self.PCT_RE.match(rollup)
                if m:
                    # We have a percentile to manage here
                    rollup = 'pct'
                    pct = int(m.group(1))
                    res = getattr(self, '_comp_{}'.format(rollup))(data, pct)
                    suffix = 'p{}'.format(pct)
                else:
                    res = getattr(self, '_comp_{}'.format(rollup))(data)
                ret['{}.{}'.format(metric_name, suffix)] = res

        return ret

    def _comp_sum(self, data: List[DataTup]) -> Union[float, int]:
        total = 0
        for dtup in data:
            if dtup.value is not None:
                total += dtup.value
            else:
                logging.error(f'Invalid None in dtup: {dtup}')

        return total

    def _comp_sumb(self, data: List[DataTup]) -> Union[float, int]:
        """
        Sum from the base, aka, use the first metric as the base and
        compute the sum from that
        """
        min_val = data[0].value
        max_val = min_val
        # Find the smallest and largest values to get the difference between
        # them
        for dtup in data:
            if dtup.value < min_val:
                min_val = dtup.value
            if dtup.value > max_val:
                max_val = dtup.value

        return max_val - min_val

    def _comp_avg(self, data: List[DataTup]) -> Union[float, int]:
        total = self._comp_sum(data)
        return total / len(data)

    def _comp_pct(
            self,
            data: List[DataTup],
            pct: int,
            ) -> Union[float, int]:
        """
        Compute the percentile specified and return it
        """
        vals = [i.value for i in data if i.value is not None]
        if not vals:
            return None
        return percentile(vals, pct)


#
# Singleton convenience items
#

# Singleton DataManager instance
DM = None


def get_datamanager(config: GDConfig=None) -> DataManager:
    # This will return the singleton, or create it if it doesn't exist
    global DM

    if DM is None:
        if config is None:
            raise InvalidConfigError(
                'The DataManager must be initialized with the config')
        DM = DataManager(config)

    return DM


# Shortcut name for get_datamanager
dm = get_datamanager

# Sample data - TODO: delete later
"""
    {
        "values": [
            196,
            0
        ],
        "dstypes": [
            "derive",
            "derive"
        ],
        "dsnames": [
            "rx",
            "tx"
        ],
        "time": 1583003789.529,
        "interval": 10.0,
        "host": "jay-vm",
        "plugin": "interface",
        "plugin_instance": "enp0s3",
        "type": "if_dropped",
        "type_instance": ""
    },

    {
        "values": [
            45761314816
        ],
        "dstypes": [
            "gauge"
        ],
        "dsnames": [
            "value"
        ],
        "time": 1583003789.529,
        "interval": 10.0,
        "host": "jay-vm",
        "plugin": "df",
        "plugin_instance": "root",
        "type": "df_complex",
        "type_instance": "used"
    },
"""
