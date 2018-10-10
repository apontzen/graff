from nose.tools import assert_raises
from graff import node
import graff


def setup():
    global testdb
    testdb = graff.Connection()
    sim_node = testdb.add_node("simulation")


def test_query_error():
    with assert_raises(node.QueryStructureError):
        testdb.query_node("simulation")["anything"]
