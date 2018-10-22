def setup():
    import test_basic_query
    test_basic_query.setup()
    global test_db
    test_db = test_basic_query.test_db

def test_edge_to_node_query():
    nodes = test_db.query_edge("has_halo").node().all()
    assert nodes==test_db.query_node("halo").all()

def test_node_to_edge_query_named_no_results():
    assert test_db.query_node("timestep").edge("has_timestep").count()==0

def test_node_to_edge_query_named():
    assert test_db.query_node("simulation").edge("has_timestep").node().all()==test_db.query_node('timestep').all()

def test_node_to_edge_query_unnamed():
    nodes = test_db.query_node("timestep").edge().node().all()
    assert nodes == test_db.query_node("halo").all()
