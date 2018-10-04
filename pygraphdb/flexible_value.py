from sqlalchemy.ext.mutable import MutableComposite


class AutoAssign:
    pass


class FlexibleValue(MutableComposite):
    def __init__(self, value_or_val_int=None, val_float=AutoAssign, val_str=AutoAssign):
        if val_float is AutoAssign:
            assert val_str is AutoAssign, "All fields except the value must be AutoAssign"
            self.set_value(value_or_val_int)
        else:
            self.val_int = value_or_val_int
            self.val_float = val_float
            self.val_str = val_str
        self._integrity_check()

    def _integrity_check(self):
        num_nones = sum([x is None for x in (self.val_int, self.val_float, self.val_str)])
        assert num_nones>=2

    def set_value(self, value):
        if isinstance(value, int):
            self.val_int = value
            self.val_float = self.val_str = None
        elif isinstance(value, float):
            self.val_float = value
            self.val_int = self.val_str = None
        elif isinstance(value, str):
            self.val_str = value
            self.val_int = self.val_float = None
        elif value is None:
            self.val_int = self.val_float = self.val_str = None
        else:
            raise TypeError("Don't know how to store a value of type %s"%type(value))
        self._integrity_check()
        self.changed()

    def get_value(self):
        self._integrity_check()
        if self.val_int is not None:
            return self.val_int
        elif self.val_float is not None:
            return self.val_float
        else:
            return self.val_str

    def __composite_values__(self):
        return self.val_int, self.val_float, self.val_str

    def __eq__(self, other):
        if isinstance(other, FlexibleValue):
            return self.get_value()==other.get_value()
        else:
            return self.get_value()==other

    def __ne__(self, other):
        return not self==other

    def __hash__(self):
        return hash(self.get_value())

    def __repr__(self):
        return repr(self.get_value())