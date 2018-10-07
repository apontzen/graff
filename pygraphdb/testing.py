from . import connection, node, edge
import random
import sys
def init_ownership_graph():

    con = connection.Connection("")

    person_node1 = con.add_node("person", {"net_worth": 1000, "name": "John McGregor"})
    person_node2 = con.add_node("person", {"net_worth": 10000, "name": "Sir Richard Stiltington"})

    for i in range(50):
        thing_node = con.add_node("thing", {"price": float(i) * 10.0, "value": float(50-i)})
        con.add_edge("owns", person_node2, thing_node)
        if i<10:
            con.add_edge("owns", person_node1, thing_node)

    return con

def init_friends_network():
    rng = random.Random(1)
    con = connection.Connection()

    people = con.add_nodes("person",1000)

    people_from = rng.choices(people, k=10000)
    people_to = rng.choices(people, k=10000)
    connections = zip(people_from, people_to)

    con.add_edges("likes", connections)
    return con