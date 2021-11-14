import sqlalchemy
import os.path
import json
from sql_server.sqlalchemy_dsl import Criterion
from sql_server.sqlalchemy_dsl import ClauseDictionaryToStatement
from sql_server.sqlalchemy_dsl import SqlAlchemyDslError
from sql_server.sqlalchemy_dsl import SqlAlchemyDslJSONEncoder
from sql_server.sqlalchemy_dsl import get_clause_from_json
from pytest import raises


def get_engine_and_metadata():
    """ returns (sqlalchemy.engine, sqlalchemy.MetaData) for testing """
    this_dir = os.path.dirname(os.path.abspath(__file__))
    test_file = os.path.join(this_dir, 'data', 'chinook.db')
    engine = sqlalchemy.create_engine("sqlite:///"+str(test_file))
    metadata = sqlalchemy.MetaData()
    metadata.reflect(engine)
    return engine, metadata


def sql_from_stmt(s):
    """ s - sqlalchemy statement
        helper function to spit out SQL string for testing
    """
    return str(s.compile(compile_kwargs={"literal_binds": True}))


def test_sql_text():
    engine, metadata = get_engine_and_metadata()
    assert set(engine.table_names()) == {'albums',
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
                                         'tracks'}
    assert set(metadata.tables.keys()) == {'albums',
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
                                           'tracks'}
    with engine.connect() as conn:
        rv = conn.execute("SELECT Name FROM media_types;")
        names = {x[0] for x in rv.fetchall()}
    assert names == {'Protected AAC audio file',
                     'MPEG audio file',
                     'AAC audio file',
                     'Protected MPEG-4 video file',
                     'Purchased AAC audio file'}


def test_simple_criterion():
    engine, metadata = get_engine_and_metadata()
    clause = {'SELECT': [Criterion('media_types', 'Name')]}
    assert str(clause['SELECT'][0].get_sqlalchemy_statement(metadata)) == "media_types.Name"

    s = ClauseDictionaryToStatement(metadata).get_statement(clause)
    assert sql_from_stmt(s) == 'SELECT media_types."Name" \nFROM media_types \nWHERE true'

    with engine.connect() as conn:
        rv = conn.execute(s)
        names = {x[0] for x in rv.fetchall()}
    assert names == {'Protected AAC audio file',
                     'MPEG audio file',
                     'AAC audio file',
                     'Protected MPEG-4 video file',
                     'Purchased AAC audio file'}

    clause = {'SELECT': [Criterion('media_types', 'Name', field_name_modifiers=['COUNT', 'DISTINCT'])]}
    assert sql_from_stmt(clause['SELECT'][0].get_sqlalchemy_statement(
        metadata)) == 'count(distinct(media_types."Name"))'

    s = ClauseDictionaryToStatement(metadata).get_statement(clause)
    assert sql_from_stmt(s) == 'SELECT count(distinct(media_types."Name")) AS count_1 \nFROM media_types \nWHERE true'

    with engine.connect() as conn:
        rv = conn.execute(s)
        count = rv.fetchall()
    assert count == [(5,)]

    clause = {'SELECT': [Criterion('media_types', 'Name')],
              'WHERE': {'AND': [Criterion('media_types', 'MediaTypeId', comparison_operator='IN', field_value=[(2, 3, 5)])]}
              }

    assert sql_from_stmt(clause['SELECT'][0].get_sqlalchemy_statement(
        metadata)) == 'media_types."Name"'
    assert sql_from_stmt(clause['WHERE']['AND'][0].get_sqlalchemy_statement(
        metadata)) == 'media_types."MediaTypeId" IN (2, 3, 5)'

    s = ClauseDictionaryToStatement(metadata).get_statement(clause)
    assert sql_from_stmt(
        s) == 'SELECT media_types."Name" \nFROM media_types \nWHERE media_types."MediaTypeId" IN (2, 3, 5)'

    with engine.connect() as conn:
        rv = conn.execute(s)
        names = {x[0] for x in rv.fetchall()}
    assert names == {'Protected AAC audio file',
                     'AAC audio file',
                     'Protected MPEG-4 video file'}

    clause = {'SELECT': [Criterion('media_types', 'MediaTypeId'), Criterion('media_types', 'Name')],
              'GROUP BY': [Criterion('media_types', 'MediaTypeId')]
              }

    s = ClauseDictionaryToStatement(metadata).get_statement(clause)
    assert sql_from_stmt(
        s) == 'SELECT media_types."MediaTypeId", media_types."Name" \nFROM media_types \nWHERE true GROUP BY media_types."MediaTypeId"'

    with engine.connect() as conn:
        rv = conn.execute(s)
        names = {x[1] for x in rv.fetchall()}
    assert names == {'Protected AAC audio file',
                     'MPEG audio file',
                     'AAC audio file',
                     'Protected MPEG-4 video file',
                     'Purchased AAC audio file'}

    clause = {}
    with raises(SqlAlchemyDslError):
        s = ClauseDictionaryToStatement(metadata).get_statement(clause)


def test_json():
    engine, metadata = get_engine_and_metadata()
    clause = {'SELECT': [Criterion('media_types', 'MediaTypeId'), Criterion('media_types', 'Name')],
              'WHERE': {'AND': [Criterion('media_types', 'MediaTypeId', comparison_operator='IN', field_value=[(2, 3, 5)])]},
              'GROUP BY': [Criterion('media_types', 'MediaTypeId')]
              }

    # test custom decoder/encoder
    s_criterion = json.dumps(clause['SELECT'][0], cls=SqlAlchemyDslJSONEncoder)
    assert s_criterion == '{"Criterion": {"table_name": "media_types", "field_name": "MediaTypeId", "field_name_modifiers": [], "comparison_operator": null, "field_value": []}}'

    simple_json_str = json.dumps({'sanity check': {'a': 1, 'b': 2}}, cls=SqlAlchemyDslJSONEncoder)

    s_clause = json.dumps(clause, cls=SqlAlchemyDslJSONEncoder)
    assert s_clause == '{"SELECT": [{"Criterion": ' +\
        '{"table_name": "media_types", "field_name": "MediaTypeId", "field_name_modifiers": [], "comparison_operator": null, "field_value": []}}, {"Criterion": ' +\
        '{"table_name": "media_types", "field_name": "Name", "field_name_modifiers": [], "comparison_operator": null, "field_value": []}}], ' +\
        '"WHERE": {"AND": [{"Criterion": {"table_name": "media_types", "field_name": "MediaTypeId", "field_name_modifiers": [], "comparison_operator": "IN", "field_value": [[2, 3, 5]]}}]}, ' +\
        '"GROUP BY": [{"Criterion": {"table_name": "media_types", "field_name": "MediaTypeId", "field_name_modifiers": [], "comparison_operator": null, "field_value": []}}]}'

    # test get_json
    assert clause['SELECT'][0].get_json() == '{"Criterion": ' +\
        '{"table_name": "media_types", "field_name": "MediaTypeId", "field_name_modifiers": [], "comparison_operator": null, "field_value": []}}'

    assert ClauseDictionaryToStatement(metadata).get_json(clause) == '{"CLAUSE": ' +\
        '{"SELECT": ' +\
        '[{"Criterion": {"table_name": "media_types", "field_name": "MediaTypeId", "field_name_modifiers": [], "comparison_operator": null, "field_value": []}}, ' +\
        '{"Criterion": {"table_name": "media_types", "field_name": "Name", "field_name_modifiers": [], "comparison_operator": null, "field_value": []}}], ' +\
        '"WHERE": {"AND": [{"Criterion": {"table_name": "media_types", "field_name": "MediaTypeId", "field_name_modifiers": [], "comparison_operator": "IN", "field_value": [[2, 3, 5]]}}]}, ' +\
        '"GROUP BY": [{"Criterion": {"table_name": "media_types", "field_name": "MediaTypeId", "field_name_modifiers": [], "comparison_operator": null, "field_value": []}}]}}'

    # test from_json
    read_criterion = Criterion.from_json(s_criterion)
    assert sql_from_stmt(read_criterion.get_sqlalchemy_statement(metadata)) == 'media_types."MediaTypeId"'

    read_clause = get_clause_from_json(s_clause)
    assert sql_from_stmt(ClauseDictionaryToStatement(metadata).get_statement(
        read_clause)) == 'SELECT media_types."MediaTypeId", media_types."Name" \n' +\
        'FROM media_types \n' +\
        'WHERE media_types."MediaTypeId" IN (2, 3, 5) GROUP BY media_types."MediaTypeId"'


def test_nested_query():
    engine, metadata = get_engine_and_metadata()
    clause = {'SELECT': [Criterion('media_types', 'Name')],
              'WHERE': {'AND': [Criterion('media_types', 'MediaTypeId', comparison_operator='IN', field_value=[(2, 3, 5)]),
                                {'OR': [Criterion('media_types', 'MediaTypeId', comparison_operator='>', field_value=[1]),
                                        Criterion('media_types', 'MediaTypeId', comparison_operator='<', field_value=[3])]
                                 },
                                Criterion('media_types', 'MediaTypeId', comparison_operator='=', field_value=[3])
                                ]
                        }
              }
    assert sql_from_stmt(ClauseDictionaryToStatement(metadata).get_statement(
        clause)) == 'SELECT media_types."Name" \n' +\
        'FROM media_types \n' + \
        'WHERE media_types."MediaTypeId" IN (2, 3, 5) AND (media_types."MediaTypeId" > 1 OR media_types."MediaTypeId" < 3) AND media_types."MediaTypeId" = 3'
