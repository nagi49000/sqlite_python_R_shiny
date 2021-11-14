from sql_server.sql_wrapper import SqlWrapper
import os.path


def test_get():
    this_dir = os.path.dirname(os.path.abspath(__file__))
    test_file = os.path.join(this_dir, 'data', 'chinook.db')
    args = ['sqlite:///'+test_file]
    kwargs = {}
    s = SqlWrapper(args, kwargs)
    assert s.get_table_count('employees') == 8
    assert s.get_table_names() == ['albums',
                                   'artists',
                                   'customers',
                                   'employees',
                                   'genres',
                                   'invoice_items',
                                   'invoices',
                                   'media_types',
                                   'playlist_track',
                                   'playlists',
                                   'sqlite_sequence',
                                   'sqlite_stat1',
                                   'tracks']
