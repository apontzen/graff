import sqlalchemy.sql.operators as op
import sqlalchemy.sql as sql

class Condition(object):
    def requires_properties(self):
        return []

    def assign_sql_columns(self, assignment_dictionary):
        """Assigns the sql column names to the query, using a dictionary mapping property name to sql column name"""
        pass

    def to_sql(self):
        """Converts this Condition to a sqlalchemy ClauseElement"""
        raise ValueError("Not a complete condition")

    def __eq__(self, other):
        return BinaryComparison(self, other, op.eq)

    def __ne__(self, other):
        return BinaryComparison(self, other, op.ne)

    def __ge__(self, other):
        return BinaryComparison(self, other, op.ge)

    def __gt__(self, other):
        return BinaryComparison(self, other, op.gt)

    def __le__(self, other):
        return BinaryComparison(self, other, op.le)

    def __lt__(self, other):
        return BinaryComparison(self, other, op.lt)


class Property(Condition):
    def __init__(self, name):
        self._name = name
        self._sql_column = sql.literal_column("column_" + name)

    def requires_properties(self):
        return [self._name]

    def assign_sql_columns(self, assignment_dictionary):
        self._sql_column = assignment_dictionary[self._name]

    def to_sql(self):
        return self._sql_column

class Value(Condition):
    def __init__(self, value):
        self._value = value

    def to_sql(self):
        return sql.literal(self._value)

def _to_condition(obj):
    if not isinstance(obj, Condition):
        return Value(obj)
    else:
        return obj

class BinaryComparison(Condition):
    def __init__(self, first, second, comparison_operator):
        self._first = _to_condition(first)
        self._second = _to_condition(second)
        self._comparison_operator = comparison_operator

    def requires_properties(self):
        return self._first.requires_properties()+self._second.requires_properties()

    def assign_sql_columns(self, assignment_dictionary):
        self._first.assign_sql_columns(assignment_dictionary)
        self._second.assign_sql_columns(assignment_dictionary)

    def to_sql(self):
        return self._comparison_operator(self._first.to_sql(), self._second.to_sql())
