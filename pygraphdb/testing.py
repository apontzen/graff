from . import initialize, node, edge

def init_ownership_graph():

    initialize("")

    person_node1 = node.add("person", {"net_worth": 1000, "name": "John McGregor"})
    person_node2 = node.add("person", {"net_worth": 10000, "name": "Sir Richard Stiltington"})

    for i in range(50):
        thing_node = node.add("thing", {"price": float(i) * 10.0, "value": float(50-i)})
        edge.add(person_node2, thing_node, "owns")
        if i<10:
            edge.add(person_node1, thing_node, "owns")