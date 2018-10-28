from graff import orm
import graff.condition as c

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

def test_named_edge_property_query():
    results = test_db.query_node("simulation").edge().return_property("test_property").\
        node().edge().return_property("test_property").all()
    assert results==[(1,2),(3,4)]

def test_edge_properties_query():
    results = test_db.query_edge("is_successor").return_properties().all()
    assert len(results)==1
    assert results[0]=={'test_property': 5, 'comment': 'test comment'}

def test_edge_filter_query():
    results = test_db.query_edge("has_timestep").filter(c.Property('test_property')>2).all()
    assert len(results)==1
    assert results[0].id==3