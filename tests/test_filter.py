import graff, graff.condition as c, graff.testing as testing


def setup():
    global test_db
    test_db = testing.init_ownership_graph()

def test_condition_with_value():
    cond = c.Property("price")>500.0
    assert cond.get_unresolved_property_names() == {"price"}
    assert str(cond.to_sql())=="column_price > :param_1"
    assert cond.to_sql().compile().params["param_1"]==500.0

def test_conditions():
    conditions = "__lt__", "__gt__", "__eq__", "__ne__", "__le__", "__ge__", \
                 "__add__", "__sub__", "__mul__", "__truediv__"
    sql_conditions = "<", ">", "=", "!=", "<=", ">=", \
                     "+", "-", "*", "/"

    for cond, sqlcond in zip(conditions, sql_conditions):
        cond = getattr(c.Property("price"), cond)(c.Property("value"))
        assert cond.get_unresolved_property_names() == {"price", "value"}
        assert str(cond.to_sql())=="column_price %s column_value"%sqlcond

def test_logic():
    not_greater = ~(c.Property("price")>c.Property("value"))
    assert str(not_greater.to_sql())=="column_price <= column_value"

    and_greater = (c.Property("price") > c.Property("value")) & (c.Property("price") > 200.0)
    assert str(and_greater.to_sql()) == "column_price > column_value AND column_price > :param_1"

    or_greater = (c.Property("price") > c.Property("value")) | (c.Property("price") > 200.0)
    assert str(or_greater.to_sql()) == "column_price > column_value OR column_price > :param_1"

    assert or_greater.get_unresolved_property_names() == {"price", "value"}


def test_filter():
    results = test_db.query_node("thing").return_this().filter(c.Property("value")>25.0).return_property("value").all()
    assert len(results)==25
    for node, value in results:
        assert value>25.0

    results = test_db.query_node("person").\
        filter(c.Property("net_worth")<5000).\
        follow("owns").\
        return_property("price").all()

    assert len(results)==10

    for price in results:
        assert price<100.0

    results = test_db.query_node("thing").filter((c.Property("value") > 25.0) & (c.Property("price")>100)).\
        return_property("value","price").all()

    assert len(results)==14
    for value, price in results:
        assert value>25.0
        assert price>100.0

def test_filter_does_not_return_values():
    results = test_db.query_node("thing").filter(c.Property("value") > 25.0).all()
    assert isinstance(results[0], graff.orm.Node)
