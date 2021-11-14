from sql_server.sql_wrapper import SqlWrapper
from sql_server.sql_wrapper import SqlWrapperException
import flask
import logging
from os import getenv


def create_app(sql_engine_args_list, sql_engine_kwargs_dict):
    app = flask.Flask(__name__)
    app.config['sql_wrapper'] = SqlWrapper(sql_engine_args_list, sql_engine_kwargs_dict)

    @app.errorhandler(SqlWrapperException)
    def handle_sql_wrapper_exception(e):
        return flask.jsonify(error=str(e)), 500

    @app.route('/debug_message')
    def debug_message():
        logging.info('SqlWrapper - debug_message')
        return 'SqlWrapper debug message'

    @app.route('/get_table_count', methods=['GET', 'POST'])
    def get_table_count():
        content = flask.request.get_json()
        table_name = content['table_name']
        logging.info('SqlWrapper - get_table_count with table_name = '+table_name)
        return flask.jsonify({
            'table_name': table_name,
            'count': app.config['sql_wrapper'].get_table_count(table_name)
        })

    @app.route('/get_table_names')
    def get_table_names():
        logging.info('SqlWrapper - get_table_names')
        return flask.jsonify({
            'table_names': app.config['sql_wrapper'].get_table_names()
        })

    return app
