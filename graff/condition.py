import operator as op
import sqlalchemy.sql as sql

class Condition(object):
    def get_unresolved_property_names(self):
        """Return the set of property names this condition requires but that are not yet bound to a temp table column"""
        return set()

    def get_resolved_property_id_columns(self):
        """Return the set of temp table columns this condition requires a value to be queried from"""
        return set()

    def assign_sql_columns(self, assignment_dictionary):
        """Assigns the sql column names to the query, using a dictionary mapping property name to sql column name"""
        pass

    def to_sql(self):
        """Converts this Condition to a sqlalchemy ClauseElement"""
        raise ValueError("Not a complete condition")

    def __eq__(self, other):
        return BinaryOperator(self, other, op.eq)

    def __ne__(self, other):
        return BinaryOperator(self, other, op.ne)

    def __ge__(self, other):
        return BinaryOperator(self, other, op.ge)

    def __gt__(self, other):
        return BinaryOperator(self, other, op.gt)

    def __le__(self, other):
        return BinaryOperator(self, other, op.le)

    def __lt__(self, other):
        return BinaryOperator(self, other, op.lt)

    def __and__(self, other):
        return BinaryOperator(self, other, op.and_)

    def __or__(self, other):
        return BinaryOperator(self, other, op.or_)

    def __mul__(self, other):
        return BinaryOperator(self, other, op.mul)

    def __rmul__(self, other):
        return BinaryOperator(other, self, op.mul)

    def __truediv__(self, other):
        return BinaryOperator(self, other, op.truediv)

    def __rtruediv__(self, other):
        return BinaryOperator(other, self, op.truediv)

    def __add__(self, other):
        return BinaryOperator(self, other, op.add)

    def __radd__(self, other):
        return BinaryOperator(other, self, op.radd)

    def __sub__(self, other):
        return BinaryOperator(self, other, op.sub)

    def __rsub__(self, other):
        return BinaryOperator(other, self, op.sub)

    def __invert__(self):
        return UnaryOperator(self, op.inv)


class Property(Condition):
    def __init__(self, name):
        self._name = name
        self._sql_column = sql.literal_column("column_" + name)

    def get_unresolved_property_names(self):
        return {self._name}

    def assign_sql_columns(self, assignment_dictionary):
        self._sql_column = assignment_dictionary[self._name]

    def to_sql(self):
        return self._sql_column

class BoundProperty(Condition):
    def __init__(self, name, sql_id_column):
        self._name = name
        self._sql_id_column = sql_id_column
        self._sql_column = sql.literal_column("column_" + name)

    def get_unresolved_property_names(self):
        return set()

    def get_resolved_property_id_columns(self):
        return {self._sql_id_column}

    def assign_sql_columns(self, assignment_dictionary):
        self._sql_column = assignment_dictionary[self._sql_id_column]

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

class BinaryOperator(Condition):
    def __init__(self, first, second, comparison_operator):
        self._first = _to_condition(first)
        self._second = _to_condition(second)
        self._comparison_operator = comparison_operator

    def get_unresolved_property_names(self):
        first = self._first.get_unresolved_property_names()
        second = self._second.get_unresolved_property_names()
        return first.union(second)

    def get_resolved_property_id_columns(self):
        first = self._first.get_resolved_property_id_columns()
        second = self._second.get_resolved_property_id_columns()
        return first.union(second)

    def assign_sql_columns(self, assignment_dictionary):
        self._first.assign_sql_columns(assignment_dictionary)
        self._second.assign_sql_columns(assignment_dictionary)

    def to_sql(self):
        return self._comparison_operator(self._first.to_sql(), self._second.to_sql())

class UnaryOperator(Condition):
    def __init__(self, underlying, operator):
        self._underlying = underlying
        self._operator = operator

    def get_unresolved_property_names(self):
        return self._underlying.get_unresolved_property_names()

    def get_resolved_property_id_columns(self):
        return self._underlying.get_resolved_property_id_columns()

    def assign_sql_columns(self, assignment_dictionary):
        self._underlying.assign_sql_columns(assignment_dictionary)

    def to_sql(self):
        return self._operator(self._underlying.to_sql())