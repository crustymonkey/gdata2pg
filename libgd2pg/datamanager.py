from .error import InvalidConfigError, InvalidDataError
from .config import GDConfig
from collections import defaultdict, namedtuple
from io import StringIO
from typing import Union, Dict, Sequence, DefaultDict, List, Any, NamedTuple
import logging


class DataManager:
    # This is an intermediate data tuple used in _get_metric
    DataTup = namedtuple('DataTup', ['name', 'type', 'value'])

    def __init__(self, config: GDConfig):
        self.config = config
        self.ent_map = self._init_map()  # Map entities to received data

    def push(self, data: Union[Dict, Sequence[Dict]]) -> None:
        """
        This will push data into the ent map.  This takes either a list
        or a single dict of data
        """
        if isinstance(dict):
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

    def get_metrics(self) -> Dict[str, Tuple[str, float]]:
        """
        This will roll up and return the aggregated metrics
        """
        pass

    def get_metrics_reset(self) -> Any:
        """
        This will get the metrics and reset the internal ent map
        """
        metrics = self.get_metrics()
        self.ent_map = self._init_map()
        return metrics

    def _init_map(self) -> DefaultDict[str, List[Dict[str, Any]]]:
        return defaultdict(list)

    def _get_metric(self, data: Dict[str, Any]) -> List[Tuple[str, str, Any]]:
        """
        Calculates the metric name from the data in the dict and returns
        a list of DataTup for each specific metric in this data instance
        """
        ret = []
        s = StringIO()
        try:
            s.write('{}'.format(data['plugin']))

            if data['plugin_instance']:
                s.write('.{}'.format(data['plugin_instance']))

            if data['plugin'] != data['type']:
                s.write('.{}'.format(data['type']))

            if data['type_instance']:
                s.write('.{}'.format(data['type_instance']))

            metric_name = s.get_value()
            for i, dsn in enumerate(data['dsnames']):
                if dsn == 'value':
                    ret.append(self.DataTup(
                        metric_name, data['dstypes'][i], data['values'][i]))
                else:
                    tmp = '{}.{}'.format(metric_name, dsn)
                    ret.append(self.DataTup(
                        tmp, data['dstypes'][i], data['values'][i]))
        except Exception:
            logging.error('Invalid data object for metric name')
            logging.debug('DATA: {}'.format(data))
            raise InvalidDataError('Invalid data: {}'.format(data))

        # If we've made it here, return the metric name
        return ret


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
