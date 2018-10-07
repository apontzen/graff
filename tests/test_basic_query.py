import pygraphdb, pygraphdb.category, pygraphdb.node, pygraphdb.edge



def setup():
    global sim_node, ts_node, ts2_node, halo_node, halo2_node, test_db
    test_db = pygraphdb.Connection()

    sim_node = test_db.add_node("simulation")
    ts_node = test_db.add_node("timestep", {'timestep_name': "ts1"})
    halo_node = test_db.add_node("halo", {'halo_number': 2})

    ts2_node = test_db.add_node("timestep", {'timestep_name': "ts2"})
    halo2_node = test_db.add_node("halo", {'halo_number': 3})

    test_db.add_edge(sim_node, ts_node, "has_timestep")
    test_db.add_edge(ts_node, halo_node, "has_halo")
    test_db.add_edge(sim_node, ts2_node, "has_timestep")
    test_db.add_edge(ts2_node, halo2_node, "has_halo")
    test_db.add_edge(halo_node, halo2_node, "is_successor")

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

def test_with_property():
    assert test_db.query_node("timestep").with_property("timestep_name").all() == [(ts_node, "ts1"), (ts2_node, "ts2")]
    assert test_db.query_node("timestep").with_property().all() == [(ts_node, "ts1"), (ts2_node, "ts2")]

def test_with_property_no_results():
    # no results with this name, but would be other results
    assert test_db.query_node("timestep").with_property("has_halo").all() == [(ts_node, None), (ts2_node, None)]

    # no results at all, with any name
    assert test_db.query_node("boring").with_property("has_halo").all() == [(boring_node, None), (boring_node2, None)]
    assert test_db.query_node("boring").with_property().all() == [(boring_node, None), (boring_node2, None)]

def test_with_property_multiple_unnamed_results():
    results = test_db.query_node("multipropertynode").with_property().all()
    expected = [(multiproperty_node,1), (multiproperty_node,"two")]
    assert (results == expected) or (results == expected[:-1])

def test_with_property_multiple_named_results():
    results = test_db.query_node("multipropertynode").with_property("property2","property1").all()
    expected = [(multiproperty_node,"two",1)]
    assert results==expected
