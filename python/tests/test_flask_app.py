from sql_server.flask_app import create_app
import os
import json


def get_test_client():
    this_dir = os.path.dirname(os.path.abspath(__file__))
    sql_data_source = 'sqlite:///' + os.path.join(this_dir, 'data', 'chinook.db')
    flask_app = create_app([sql_data_source], {})
    return flask_app.test_client()


def test_debugMessage():
    test_client = get_test_client()
    rv = test_client.get('/debug_message')
    assert rv.get_data() == b'SqlWrapper debug message'


def test_get_table_names():
    test_client = get_test_client()
    rv = test_client.get('/get_table_names')
    content = json.loads(rv.get_data())
    assert content == {'table_names': ['albums', 'artists', 'customers', 'employees', 'genres', 'invoice_items',
                                       'invoices', 'media_types', 'playlist_track', 'playlists', 'sqlite_sequence',
                                       'sqlite_stat1', 'tracks']}


def test_get_table_count():
    test_client = get_test_client()
    json_in = json.dumps({'table_name': 'employees'})
    rv = test_client.post('/get_table_count', data=json_in, content_type='application/json')
    content = json.loads(rv.get_data())
    assert content == {'count': 8, 'table_name': 'employees'}

    json_in = json.dumps({'table_name': 'non_existant_table'})
    rv = test_client.post('/get_table_count', data=json_in, content_type='application/json')
    assert rv.status_code == 500
    content = json.loads(rv.get_data())
    assert content == {'error': 'nonexistant table: non_existant_table'}
