from nose.tools import assert_raises
from pygraphdb import node
import pygraphdb


def setup():
    global testdb
    testdb = pygraphdb.Connection()
    sim_node = testdb.add_node("simulation")


def test_query_error():
    with assert_raises(node.QueryStructureError):
        testdb.query_node("simulation")["anything"]
