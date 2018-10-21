import graff, graff.category, graff.testing


def test_add_nodes():
    test_db = graff.Connection()
    test_db.add_nodes("test_category", 10, properties=[{"test_int": i, "test_float": float(i), "test_str": "test_%d"%i} for i in range(10)])
    assert test_db.query_node("test_category").count()==10
    test_db.add_nodes("test_category", 10,
                      properties=[{"test_int": i, "test_float": float(i), "test_str": "test_%d" % i} for i in
                                  range(10,20)])
    vals = test_db.query_node("test_category").return_property("test_int", "test_float", "test_str").all()
    for i, row in enumerate(vals):
        assert row[0]==i
        assert row[1]==float(i)
        assert row[2]=="test_%d"%i

def test_add_node():
    test_db = graff.Connection()
    test_db.add_node("test_category", {"test_int":1, "test_float": 0.5, "test_str": "test_1"})
    test_db.add_node("test_category", {"test_int":2, "test_float": 1.0, "test_str": "test_2"})
    vals = test_db.query_node("test_category").return_property("test_int", "test_float", "test_str").all()

    for i, row in enumerate(vals):
        assert row[0]==i+1
        assert row[1]==float(i+1)*0.5
        assert row[2]=="test_%d"%(i+1)

def test_add_edges():
    test_db = graff.Connection()
    test_db.add_nodes("test_node", 10)
    test_db.add_edges("test_edge", [(1,2), (2,3), (3,4)], properties=[{'edge_strength': 1.0},
                                                                      {'edge_strength': 2.0},
                                                                      {'edge_strength': 3.0}])
    result = test_db.query_edge("test_edge").all()
    graff.testing.assert_edge_connections(result, [(1,2), (2,3), (3,4)])
    assert dict(result[0])=={'edge_strength': 1.0}