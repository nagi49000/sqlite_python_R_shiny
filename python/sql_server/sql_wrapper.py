from sqlalchemy.engine import create_engine


class SqlWrapperException(ValueError):
    pass


class SqlWrapper:
    def __init__(self, engine_args_list, engine_kwargs_dict):
        self._engine = create_engine(*engine_args_list, **engine_kwargs_dict)

    def get_table_count(self, table):
        if table in self._engine.table_names():
            return self._engine.execute("SELECT COUNT(*) FROM " + table + ";").fetchone()[0]
        else:
            raise SqlWrapperException('nonexistant table: '+table)

    def get_table_names(self):
        return self._engine.table_names()
