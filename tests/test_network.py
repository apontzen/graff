from graff import testing

def setup():
    global db
    db = testing.init_friends_network()

def test_friends_of_friends():
    friends_of_friends = db.query_node("person").return_this().follow().follow().all()
    expected_fof = [68, 272, 632, 745, 403, 34, 608, 987, 451, 286]
    expected_sources = [1, 1, 1, 2, 2, 3, 3, 4, 4, 4]

    assert len(friends_of_friends) == 99702

    for e_fof, e_source, a_result in zip(expected_fof, expected_sources, friends_of_friends[:500:50]):
        assert e_fof== a_result[0].id
        assert e_source== a_result[1].id

