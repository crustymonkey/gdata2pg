# 
# This is a library for building the JSON to submit to this server
# This handles key/values and submits them to the specified server
#

import json
import logging
import time
from socket import gethostname
from typing import List
from urllib.request import (
    HTTPBasicAuthHandler,
    build_opener,
    HTTPPasswordMgrWithDefaultRealm,
)

class Gdata:

    def __init__(
            self,
            plugin: str,
            dstypes: List[str],
            values: List[float],
            host: str=None,
            plugin_instance: str='',
            dtype: str=None,
            dtype_instance: str='',
            dsnames: List[str]=None,
            interval: float=10.0) -> None:
        self.plugin = plugin
        self.dstypes = dstypes
        self.values = values
        self.plugin_instance = plugin_instance
        self.dtype = dtype
        self.dtype_instance = dtype_instance
        self.dsnames = dsnames
        self.interval = interval
        self.host = host if host else self._get_hostname()

    def to_dict(self):
        ret = {
            'values': self.values,
            'dstypes': self.dstypes,
            'dsnames': self.dsnames if self.dsnames else ['value'],
            'time': time.time(),
            'interval': self.interval,
            'host': self.host,
            'plugin': self.plugin,
            'plugin_instance': self.plugin_instance,
            'type': self.dtype if self.dtype else self.plugin,
            'type_instance': self.dtype_instance,
        }

        return ret

    def _get_hostname(self):
        name = gethostname()
        if '.' in name:
            name = name.split('.')[0]

        return name


class GdataSubmit:

    def __init__(self, username: str, password: str, url: str):
        self.username = username
        self.password = password
        self.url = url

    def send_data(self, data: List[Gdata]) -> bool:
        ret = True
        handler = self._get_auth_handler()
        opener = build_opener(self._get_auth_handler())

        resp = None
        
        try:
            resp = opener.open(
                self.url,
                json.dumps([d.to_dict() for d in data]).encode('utf-8'),
                timeout=5,
            )
        except Exception:
            logging.exception('Failed to open url: {}'.format(self.url))
            ret = False

        if resp and resp.getcode() != 200:
            logging.error(
                'Sending data returned code: {}'.format(resp.getcode()))
            ret = False

        return ret

    def _get_auth_handler(self) -> HTTPBasicAuthHandler:
        pmgr = HTTPPasswordMgrWithDefaultRealm()
        pmgr.add_password(
            None,
            self.url,
            self.username,
            self.password,
        )
        handler = HTTPBasicAuthHandler(pmgr)

        return handler


