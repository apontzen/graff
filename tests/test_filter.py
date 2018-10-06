import pygraphdb, pygraphdb.node, pygraphdb.edge, pygraphdb.condition as c

def setup():
    global sim_node, ts_node, ts2_node, halo_node, halo2_node
    pygraphdb.initialize("")

    person_node1 = pygraphdb.node.add("person", {"net_worth": 1000})
    person_node2 = pygraphdb.node.add("person", {"net_worth": 10000})

    for i in range(50):
        thing_node = pygraphdb.node.add("thing", {"price": float(i) * 10.0, "value": float(50-i)})
        pygraphdb.edge.add(person_node2, thing_node, "owns")
        if i<10:
            pygraphdb.edge.add(person_node1, thing_node, "owns")

def test_condition_with_value():
    cond = c.Property("price")>500.0
    assert cond.requires_properties()==["price"]
    assert str(cond.to_sql())=="column_price > :param_1"
    assert cond.to_sql().compile().params["param_1"]==500.0

def test_conditions():
    conditions = "__lt__", "__gt__", "__eq__", "__ne__", "__le__", "__ge__"
    sql_conditions = "<", ">", "=", "!=", "<=", ">="

    for cond, sqlcond in zip(conditions, sql_conditions):
        cond = getattr(c.Property("price"), cond)(c.Property("value"))
        assert cond.requires_properties() == ["price", "value"]
        assert str(cond.to_sql())=="column_price %s column_value"%sqlcond

def test_filter():
    results = pygraphdb.node.query("thing").filter(c.Property("value")>25.0).all()
    assert len(results)==25
    for node, value in results:
        assert value>25.0

    results = pygraphdb.node.query("person").\
        filter(c.Property("net_worth")<5000).\
        follow("owns").\
        with_property("price").all()

    assert len(results)==10

    for node,price in results:
        assert price<100.0