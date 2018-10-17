from nose.tools import assert_raises
import graff


def setup():
    global testdb
    testdb = graff.Connection()
    sim_node = testdb.add_node("simulation")


def test_query_error():
    with assert_raises(graff.query.base.QueryStructureError):
        testdb.query_node("simulation")["anything"]
