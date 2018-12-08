import functools
from sqlalchemy.orm.properties import CompositeProperty
from sqlalchemy import func

def flexible_set_value(object, value, attr=True, null_others=True):
    """Given an object, set either value_int, value_str, or value_float as appropriate.

    :param attr: if True, the attribute is set. If False, the dictionary value is set instead.
    :param null_others: if True, the remaining values are set to None. If False, they are ignored.
    """
    all_names = ['value_float', 'value_int', 'value_str']

    if isinstance(value, float):
        assigned_name = 'value_float'
    elif isinstance(value, int):
        assigned_name = 'value_int'
    elif isinstance(value, str):
        assigned_name = 'value_str'
    else:
        raise TypeError("Unable to assign this value to any of "+str(all_names))

    if attr:
        set_function = object.__setattr__
    else:
        set_function = object.__setitem__

    set_function(assigned_name, value)

    if null_others:
        for other_name in all_names:
            if other_name!=assigned_name:
                set_function(other_name, None)

class FlexibleOperators(object):
    def _get_composite_values_or_elements(self):
        pass

    @classmethod
    def _coalesce(self, *args):
        return False

    def _op_or_None(self, a, b, op):
        if a is None:
            return None
        return getattr(a, op)(b)

    def _intelligent_operator(self, other, op="__gt__"):
        if hasattr(other, "__composite_values__"):
            return self._coalesce(*[self._op_or_None(a, b, op) for a, b in
                                    zip(self._get_composite_values_or_elements(), other.__composite_values__())])
        else:
            return self._coalesce(*[self._op_or_None(a, other, op) for a in self._get_composite_values_or_elements()])


for op in "gt", "lt", "ge", "le", "eq", "ne", "div", "mul", "truediv", \
          "rtruediv", "rdiv", "rmul", "add", "sub", "radd", "rsub":
    opname = "__" + op + "__"
    setattr(FlexibleOperators, opname, functools.partialmethod(FlexibleOperators._intelligent_operator,
                                                               op=opname))


class FlexibleStatementComparator(FlexibleOperators, CompositeProperty.Comparator):
    _coalesce = func.coalesce

    def _get_composite_values_or_elements(self):
        return self.__clause_element__().clauses


class FlexibleValue(FlexibleOperators):
    @classmethod
    def _coalesce(cls, *args):
        for a in args:
            if a is not None:
                return a

    def __init__(self, value_int, value_float, value_str):
        self.value_int = value_int
        self.value_float = value_float
        self.value_str = value_str

    def __composite_values__(self):
        return self.value_int, self.value_float, self.value_str

    def _get_composite_values_or_elements(self):
        return self.__composite_values__()

    @property
    def value(self):
        return self._coalesce(self.value_int, self.value_float, self.value_str)

    def __repr__(self):
        return self.value

    def __str__(self):
        return str(self.value)

    def __int__(self):
        return int(self.value)

    def __float__(self):
        return float(self.value)

    def __eq__(self, other):
        return self.value == other

    def __hash__(self):
        return hash(self.value)

    @value.setter
    def value(self, val):
        flexible_set_value(self, val)