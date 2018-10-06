import pygraphdb, pygraphdb.category, pygraphdb.node, pygraphdb.edge



def setup():
    global sim_node, ts_node, ts2_node, halo_node, halo2_node
    pygraphdb.initialize("")

    sim_node = pygraphdb.node.add("simulation")
    ts_node = pygraphdb.node.add("timestep", {'timestep_name': "ts1"})
    halo_node = pygraphdb.node.add("halo", {'halo_number': 2})

    ts2_node = pygraphdb.node.add("timestep", {'timestep_name': "ts2"})
    halo2_node = pygraphdb.node.add("halo", {'halo_number': 3})

    pygraphdb.edge.add(sim_node, ts_node, "has_timestep")
    pygraphdb.edge.add(ts_node, halo_node, "has_halo")
    pygraphdb.edge.add(sim_node, ts2_node, "has_timestep")
    pygraphdb.edge.add(ts2_node, halo2_node, "has_halo")

    pygraphdb.edge.add(halo_node, halo2_node, "is_successor")

    global boring_node, boring_node2
    boring_node = pygraphdb.node.add("boring")
    boring_node2 = pygraphdb.node.add("boring")

    global multiproperty_node
    multiproperty_node = pygraphdb.node.add("multipropertynode", {'property1': 1, 'property2': "two"})


def test_query_node():
    assert pygraphdb.node.query("simulation").all() == [sim_node]
    assert pygraphdb.node.query("halo").all() == [halo_node, halo2_node]

def test_follow():
    assert pygraphdb.node.query("timestep").follow("has_halo").all() == [halo_node, halo2_node]
    assert pygraphdb.node.query("timestep").follow("has_halo").follow("is_successor").all() == [halo2_node]
    assert pygraphdb.node.query("timestep").follow("has_halo").follow("has_timestep").all() == []

def test_follow_no_category():
    assert pygraphdb.node.query("timestep").follow().follow().all() == [halo2_node]

def test_with_property():
    assert pygraphdb.node.query("timestep").with_property("timestep_name").all() == [(ts_node, "ts1"), (ts2_node, "ts2")]
    assert pygraphdb.node.query("timestep").with_property().all() == [(ts_node, "ts1"), (ts2_node, "ts2")]

def test_with_property_no_results():
    # no results with this name, but would be other results
    assert pygraphdb.node.query("timestep").with_property("has_halo").all() == [(ts_node, None), (ts2_node, None)]

    # no results at all, with any name
    assert pygraphdb.node.query("boring").with_property("has_halo").all() == [(boring_node, None), (boring_node2, None)]
    assert pygraphdb.node.query("boring").with_property().all() == [(boring_node, None), (boring_node2, None)]

def test_with_property_multiple_unnamed_results():
    results = pygraphdb.node.query("multipropertynode").with_property().all()
    expected = [(multiproperty_node,1), (multiproperty_node,"two")]
    assert (results == expected) or (results == expected[:-1])

def test_with_property_multiple_named_results():
    results = pygraphdb.node.query("multipropertynode").with_property("property2","property1").all()
    expected = [(multiproperty_node,"two",1)]
    assert results==expected
