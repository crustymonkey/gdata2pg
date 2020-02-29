from .config import GDConfig
from flask import Flask, request, Response
from flask_httpauth import HTTPBasicAuth
import json
import logging


APP = Flask('gdata2pg')
ARGS = None
AUTH = HTTPBasicAuth()
CONF = None
INITIALIZED = False


def _handle_post():
    raw_data = request.get_data()
    data = json.loads(raw_data)


@AUTH.verify_password
def verify_password(user, pwd):
    if user in CONF['users']:
        return CONF['users'][user] == pwd
    return False


@APP.route('/', methods=['GET', 'POST'])
@AUTH.login_required
def index():
    if request.method == 'GET':
        return 'Hello, {}\n'.format(AUTH.username())
    else:
        try:
            _handle_post()
        except Exception:
            logging.exception('Error in JSON decode')
            return Response('Invalid Request\n', status=400)
        return 'ok\n'


def flask_init(args):
    """
    Initialize the globals for use
    """
    global ARGS, CONF, INITIALIZED 

    INITIALIZED = True
    ARGS = args
    CONF = GDConfig()
    CONF.read(args.config)
