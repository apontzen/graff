import graff, graff.category



def setup():
    global sim_node, ts_node, ts2_node, halo_node, halo2_node, test_db, props_for_ts_node, props_for_ts2_node
    test_db = graff.testing.get_test_connection()

    sim_node = test_db.add_node("simulation")
    props_for_ts_node = {'timestep_name': "ts1", 'dummy_property_1': "dp1 for ts1", "dummy_property_2": "dp2 for ts1"}
    ts_node = test_db.add_node("timestep", props_for_ts_node)
    halo_node = test_db.add_node("halo", {'halo_number': 2})

    props_for_ts2_node = {'timestep_name': "ts2", "dummy_property_1": "dp1 for ts2"}
    ts2_node = test_db.add_node("timestep", props_for_ts2_node)
    halo2_node = test_db.add_node("halo", {'halo_number': 3})

    test_db.add_edge("has_timestep", sim_node, ts_node, {'test_property': 1})
    test_db.add_edge("has_halo", ts_node, halo_node, {'test_property': 2})
    test_db.add_edge("has_timestep", sim_node, ts2_node, {'test_property': 3})
    test_db.add_edge("has_halo", ts2_node, halo2_node, {'test_property': 4})
    test_db.add_edge("is_successor", halo_node, halo2_node, {'test_property': 5, 'comment': 'test comment'})

    global boring_node, boring_node2
    boring_node = test_db.add_node("boring")
    boring_node2 = test_db.add_node("boring")

    global multiproperty_node
    multiproperty_node = test_db.add_node("multipropertynode", {'property1': 1, 'property2': "two"})


def test_query_node():
    assert test_db.query_node("simulation").all() == [sim_node]
    assert test_db.query_node("halo").all() == [halo_node, halo2_node]

def test_follow():
    assert test_db.query_node("timestep").follow("has_halo").all() == [halo_node, halo2_node]
    assert test_db.query_node("timestep").follow("has_halo").follow("is_successor").all() == [halo2_node]
    assert test_db.query_node("timestep").follow("has_halo").follow("has_timestep").all() == []

def test_follow_no_category():
    assert test_db.query_node("timestep").follow().follow().all() == [halo2_node]

def test_return_property():
    assert test_db.query_node("timestep").return_property("timestep_name").all() == ["ts1", "ts2"]

def test_return_properties():
    x = test_db.query_node("timestep").return_properties().all()
    assert x == [props_for_ts_node, props_for_ts2_node]

def test_return_this():
    assert test_db.query_node("timestep").return_this().all() == [ts_node, ts2_node]

def test_return_this_and_property():
    assert test_db.query_node("timestep").return_this().return_property("timestep_name").all() == [(ts_node, "ts1"), (ts2_node, "ts2")]

def test_return_this_and_properties():
    assert test_db.query_node("timestep").return_this().return_properties().all() == [(ts_node, props_for_ts_node), (ts2_node, props_for_ts2_node)]

def test_return_property_no_results():
    # no results with this name, but would be other results
    assert test_db.query_node("timestep").return_this().return_property("has_halo").all() == [(ts_node, None), (ts2_node, None)]
    assert test_db.query_node("timestep").return_property("has_halo").all() == [None, None]

    # no results at all, with any name
    assert test_db.query_node("boring").return_this().return_property("has_halo").all() == [(boring_node, None), (boring_node2, None)]

def test_return_properties_no_results():
    assert test_db.query_node("boring").return_this().return_properties().all() == [(boring_node, {}), (boring_node2, {})]

def test_return_property_multiple_named_results():
    results = test_db.query_node("multipropertynode").return_this().return_property("property2","property1").all()
    expected = [(multiproperty_node,"two",1)]
    assert results==expected

