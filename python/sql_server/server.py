from cheroot.wsgi import Server as WSGIServer
from cheroot.wsgi import PathInfoDispatcher
from sql_server.flask_app import create_app
import os.path
import os
from argparse import ArgumentParser
import logging
import time
import sys


def _parse_args(cmd_line_args):
    this_dir = os.path.abspath(os.path.dirname(__file__))
    parser = ArgumentParser()
    parser.add_argument('--sql_source',
                        default='sqlite:///'+os.path.join(this_dir, '..', 'tests', 'data', 'chinook.db'),
                        help='SQL data source as a string; used to create SQLAlchemy engine')
    parser.add_argument('--url', default='127.0.0.1', help='URL of server')
    parser.add_argument('--port', default=8000, type=int, help='port number of server')
    parser.add_argument('--log', default='sql_server.log', help='file name and path for output log file')
    args = vars(parser.parse_args(cmd_line_args))
    return args


def _get_logger(args):
    logging.basicConfig(filename=args['log'], level=logging.INFO,
                        format='%(asctime)s %(message)s', datefmt="%Y-%m-%d %H:%M:%S GMT")
    logging.Formatter.converter = time.gmtime
    logger = logging.getLogger('sql_server')
    logging.info('PID: '+str(os.getpid()))
    logging.info('started with options: '+str(args))
    return logger


def get_server(cmd_line_args):
    args = _parse_args(cmd_line_args)
    logger = _get_logger(args)
    a = create_app([args['sql_source']], {})
    d = PathInfoDispatcher({'/': a})
    server = WSGIServer((args['url'], args['port']), d)
    return logger, server, a


def main_function(cmd_line_args=[]):
    logger, server, _ = get_server(cmd_line_args)
    try:
        logging.info('server started')
        server.start()
    except KeyboardInterrupt:
        server.stop()
        logging.info('server stopped')


if __name__ == '__main__':
    main_function(sys.argv[1:])
