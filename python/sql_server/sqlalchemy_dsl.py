import sqlalchemy as sa
import os.path
import json


class SqlAlchemyDslError(ValueError):
    pass


class SqlAlchemyDslJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Criterion):
            return obj.get_dict()
        else:
            return json.JSONEncoder.default(self, obj)


class SqlAlchemyDslJSONDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj):
        k = obj.keys()
        if 'Criterion' in k and len(k) == 1:
            obj = Criterion.from_dict(obj)
        return obj


def get_clause_from_json(j):
    return json.loads(j, cls=SqlAlchemyDslJSONDecoder)


class Criterion:
    """ represents a single attribute in an SQL query, e.g. a single variable in a SELECT, WHERE or GROUP BY statement """

    def __init__(self, table_name, field_name, field_name_modifiers=[], comparison_operator=None, field_value=[]):
        """ table_name - str - name of the table the field belongs to
            field_name - str
            field_name_modifiers - list<str> - list of modifiers (e.g. 'COUNT' or 'DISTINCT') to apply onto field.
                                               Modifiers are applied right to left
                                               (so [mod1, mod2, mod3] is applied as mod1(mod2(mod3(field))) )
            comparison_operator - str - operator to apply with field value, e.g. '='
            field_value - list<relevant data type> - field will be compared to these values;
                                                     list since some comparisons (e.g. between) require multiples
        """
        self._table_name = table_name
        self._field_name = field_name
        self._field_name_modifiers = field_name_modifiers
        self._comparison_operator = comparison_operator
        self._field_value = field_value
        # dictionary to map strings to sqlalchemy functions
        self._field_modifiers = {'COUNT': sa.func.count,
                                 'DISTINCT': sa.func.distinct}
        # dictionary to map strings to sqlalchemy functions
        self._comparisons = {'=': lambda x, v: x == v,
                             '==': lambda x, v: x == v,
                             '>': lambda x, v: x > v,
                             '>=': lambda x, v: x >= v,
                             '<': lambda x, v: x < v,
                             '<=': lambda x, v: x <= v,
                             '!=': lambda x, v: x != v,
                             'BETWEEN': sa.between,
                             'CONTAINS': lambda x, s: x.contains(s),
                             'LIKE': lambda x, s: x.like(s),
                             'IN': lambda x, v_list: x.in_(v_list)}

    def from_json(j):
        """ returns a 'Criterion' from the supplied json """
        d = json.loads(j)
        return Criterion.from_dict(d)

    def from_dict(d):
        """ returns a 'Criterion' from the supplied dict """
        c = d['Criterion']
        return Criterion(c['table_name'],
                         c['field_name'],
                         c['field_name_modifiers'],
                         c['comparison_operator'],
                         c['field_value'])

    def get_json(self):
        return json.dumps(self, cls=SqlAlchemyDslJSONEncoder)

    def get_dict(self):
        """ return the inits of the object as a dict """
        return {'Criterion': {'table_name': self._table_name,
                              'field_name': self._field_name,
                              'field_name_modifiers': self._field_name_modifiers,
                              'comparison_operator': self._comparison_operator,
                              'field_value': self._field_value}}

    def get_sqlalchemy_statement(self, metadata):
        """ metadata - 'sqlalchemy.sql.schema.MetaData' - metadata for the SQL database on which the query will be run
            returns an SQLAlchemy statement
        """
        stmt = metadata.tables[self._table_name].columns[self._field_name]
        for modifier in self._field_name_modifiers[::-1]:
            stmt = self._field_modifiers[modifier](stmt)
        if self._comparison_operator:
            stmt = self._comparisons[self._comparison_operator](stmt, *self._field_value)
        return stmt


class ClauseDictionaryToStatement:
    """ takes a specified dictionary containing Criterion, and generates a SQLAlchemy statement """

    def __init__(self, metadata):
        self._metadata = metadata
        # dictionary to map strings to sqlalchemy functions
        self._conjunctions = {'AND': sa.and_,
                              'OR': sa.or_}

    def get_json(self, clause):
        d = {'CLAUSE': clause}
        return json.dumps(d, cls=SqlAlchemyDslJSONEncoder)

    def _get_expanded_conjunction(self, criterions_for_conjunction, default_wrapper_conjuction='OR'):
        """ criterions_for_conjunction - dict<key=str, value=list<Criterion or dict> - key
                 is a conjunction paramater (e.g. 'OR' or 'AND') and the elements of the value list
                 can be a mix of Criterion or further nested conjunction_dict. Ideally, the supplied
                 dictionary should contain one element
            default_wrapper_conjuction - str - if the dictionary contains more than one element,
                                               use this conjunction to combine them
        """
        def _to_sqlalchemy(x):
            if isinstance(x, Criterion):
                return x.get_sqlalchemy_statement(self._metadata)
            else:
                return self._get_expanded_conjunction(x)  # recursive call for nested conditions

        # convert each Criterion in the tree to an sqlalchemy statement
        statements = {k: [_to_sqlalchemy(x) for x in v]
                      for k, v
                      in criterions_for_conjunction.items()}
        # apply the key in the dict as a conjunction over the sql statements
        conjuctions = [self._conjunctions[k](*v)
                       for k, v
                       in statements.items()]  # ideally of length 1
        # in case the supplied dict is of length > 1, apply a default conjunction over the dict elements
        expanded_conjunction = self._conjunctions[default_wrapper_conjuction](*conjuctions)
        return expanded_conjunction

    def get_statement(self, clause):
        """ clause - dict - SQL query dressed as a dictionary
            returns an SQLAlchemy statement
        """
        m = self._metadata
        if 'SELECT' in clause.keys():
            selects = [x.get_sqlalchemy_statement(m) for x in clause['SELECT']]
        else:
            raise SqlAlchemyDslError('SELECT not found in supplied clause')
        if 'GROUP BY' in clause.keys():
            group_bys = [x.get_sqlalchemy_statement(m) for x in clause['GROUP BY']]
        else:
            group_bys = []  # does nothing
        if 'WHERE' in clause.keys():
            wheres = self._get_expanded_conjunction(clause['WHERE'])
        else:
            wheres = True  # does nothing
        statement = sa.select(selects).where(wheres).group_by(*group_bys)
        return statement
