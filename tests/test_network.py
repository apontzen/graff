from graff import testing

def setup():
    global db
    db = testing.init_friends_network()

def test_friends_of_friends():
    friends_of_friends = db.query_node("person").with_this().follow().follow().all()
    expected_fof = [868, 112, 714, 556, 829, 139, 946, 663, 306, 33]
    expected_sources = [1, 1, 2, 3, 4, 4, 5, 5, 5, 6]

    for e_fof, e_source, a_result in zip(expected_fof, expected_sources, friends_of_friends[:500:50]):
        assert e_fof== a_result[0].id
        assert e_source== a_result[1].id

    assert len(friends_of_friends)==99991