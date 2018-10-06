from nose.tools import assert_raises
from pygraphdb import node
import pygraphdb


def setup():
    global sim_node, ts_node, ts2_node, halo_node, halo2_node
    pygraphdb.initialize("")

    sim_node = pygraphdb.node.add("simulation")


def test_query_error():
    with assert_raises(node.QueryStructureError):
        node.query("simulation").with_property("simulation").with_property("simulation")