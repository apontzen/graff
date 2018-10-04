from pygraphdb.flexible_value import FlexibleValue

test_values = [1.0, "hello", 42]

def test_flexible_value():
    x = FlexibleValue()
    for val in test_values:
        x.set_value(val)
        assert x.get_value()==val

def test_flexible_value_composite():
    x = FlexibleValue()
    for val in test_values:
        x.set_value(val)
        y = FlexibleValue(*x.__composite_values__())
        assert y.get_value()==val