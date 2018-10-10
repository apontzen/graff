import graff, graff.node as n, graff.edge, graff.condition as c, graff.testing as testing

def setup():
    global test_db
    test_db = testing.init_ownership_graph()

def test_column_transfer():
    results = test_db.query_node("person").with_property("net_worth").follow("owns").with_property("price").all()
    assert len(results)==60 # John owns 10 things, Richard owns 50 things
    assert len(results[0])==3
    for node, net_worth, price in results:
        assert net_worth==1000 or net_worth==10000
        assert price<net_worth/10

def test_column_all_property_transfer():
    results = test_db.query_node("person").with_property().follow("owns").with_property("price").all()
    assert len(results[0]) == 3
    assert len(results)==120
    for node, net_worth_or_name, price in results:
        assert net_worth_or_name in [1000, 10000, "John McGregor", "Sir Richard Stiltington"]

def test_column_backreference_in_filter():
    q1 = test_db.query_node("person").with_property("net_worth", "name")
    q2 = q1.follow("owns").with_property("price")
    q2 = q2.filter(q1['net_worth'] > 100*q2["price"])
    results = q2.all()
    assert len(results)==11 # John owns 1 thing less than 1/100th of his net worth; Richard owns 10
    for node, net_worth, name, price in results:
        assert net_worth>100*price

def test_mixed_model_column_comparison():
    q1 = test_db.query_node("person").with_property("net_worth", "name")
    q2 = q1.follow("owns").filter(q1['net_worth'] > 100*c.Property("price")).with_property("price")
    results = q2.all()
    assert len(results)==11 # John owns 1 thing less than 1/100th of his net worth; Richard owns 10
    for node, net_worth, name, price in results:
        assert net_worth>100*price

