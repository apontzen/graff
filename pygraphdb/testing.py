from . import connection, node, edge

def init_ownership_graph():

    con = connection.Connection("")

    person_node1 = con.add_node("person", {"net_worth": 1000, "name": "John McGregor"})
    person_node2 = con.add_node("person", {"net_worth": 10000, "name": "Sir Richard Stiltington"})

    for i in range(50):
        thing_node = con.add_node("thing", {"price": float(i) * 10.0, "value": float(50-i)})
        con.add_edge(person_node2, thing_node, "owns")
        if i<10:
            con.add_edge(person_node1, thing_node, "owns")

    return con