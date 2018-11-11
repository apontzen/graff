import graff.condition as c, graff.testing as testing

def setup():
    global test_db
    test_db = testing.init_ownership_graph()

def test_column_transfer():
    results = test_db.query_node("person").return_property("net_worth").follow("owns").return_property("price").all()
    assert len(results)==60 # John owns 10 things, Richard owns 50 things
    assert len(results[0])==2
    for net_worth, price in results:
        assert net_worth==1000 or net_worth==10000
        assert price<net_worth/10

def test_column_all_property_transfer():
    results = test_db.query_node("person").return_properties().follow("owns").return_property("price").all()
    assert len(results[0]) == 2
    assert (results[1][0]=={'net_worth': 1000, 'name': 'John McGregor'})


def test_column_backreference_in_filter():
    q1 = test_db.query_node("person").return_property("net_worth", "name")
    q2 = q1.follow("owns").return_property("price")
    q2 = q2.filter(q1['net_worth'] > 100*q2["price"])
    results = q2.all()
    assert len(results)==11 # John owns 1 thing less than 1/100th of his net worth; Richard owns 10
    for net_worth, name, price, final_node in results:
        assert net_worth>100*price

def test_mixed_model_column_comparison():
    q1 = test_db.query_node("person").return_property("net_worth", "name")
    q2 = q1.follow("owns").filter(q1['net_worth'] > 100*c.Property("price")).return_property("price")
    results = q2.all()
    assert len(results)==11 # John owns 1 thing less than 1/100th of his net worth; Richard owns 10
    for net_worth, name, price in results:
        assert net_worth>100*price

