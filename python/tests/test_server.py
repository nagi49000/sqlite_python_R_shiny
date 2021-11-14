import requests
from multiprocessing.pool import ThreadPool
from sql_server.server import get_server


def test_app_debug_message():
    _, _, a = get_server([])
    test_client = a.test_client()
    rv = test_client.get('/debug_message')
    assert rv.get_data() == b'SqlWrapper debug message'


def test_server_debug_message():
    pool = ThreadPool(processes=2)
    _, s, _ = get_server([])
    s.prepare()
    #rv = requests.get('http://127.0.0.1:8000/debug_message')
    async_result = pool.apply_async(requests.get, ('http://127.0.0.1:8000/debug_message',))
    pool.apply_async(s.tick)
    rv = async_result.get()
    assert rv.text == 'SqlWrapper debug message'
    s.stop()
