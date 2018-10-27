from graff import orm

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

def test_persistent_edge_query():
    results = test_db.query_node("simulation").edge("has_timestep").return_this().node().all()
    assert [x.id for x in results[0]]==[1, 2]
    assert [x.id for x in results[1]]==[3, 4]
    assert [type(x) for x in results[0]]==[orm.Edge, orm.Node]
    assert [type(x) for x in results[1]]==[orm.Edge, orm.Node]

def test_persistent_node_and_edge_query():
    results = test_db.query_node("simulation").return_this().\
        edge("has_timestep").return_this().node().all()

    assert [x.id for x in results[0]]==[1, 1, 2]
    assert [x.id for x in results[1]]==[1, 3, 4]
    assert [type(x) for x in results[0]]==[orm.Node, orm.Edge, orm.Node]
    assert [type(x) for x in results[1]]==[orm.Node, orm.Edge, orm.Node]
