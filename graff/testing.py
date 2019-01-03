from . import connection, orm
import random
import os
from sqlalchemy import create_engine

def _wipe_database(uri):
    if '//' not in uri:
        uri = 'sqlite:///' + uri
    import gc
    gc.collect() # help ensure there are no dangling connections that would cause a deadlock
    engine = create_engine(uri)
    orm.Base.metadata.drop_all(engine)
    engine.dispose()

def get_test_connection(test_uri=None):
    if test_uri is None:
        test_uri = os.environ.get('GRAFF_TEST_DATABASE_URI','')
    # wipe the database in case it already exists:
    _wipe_database(test_uri)
    return connection.Connection(test_uri)

def init_ownership_graph(db_uri=None):

    con = get_test_connection(db_uri)

    person_node1 = con.add_node("person", {"net_worth": 1000.0, "name": "John McGregor"})
    person_node2 = con.add_node("person", {"net_worth": 10000.0, "name": "Sir Richard Stiltington"})

    for i in range(50):
        thing_node = con.add_node("thing", {"price": float(i) * 10.0, "value": float(50-i)})
        con.add_edge("owns", person_node2, thing_node)
        if i<10:
            con.add_edge("owns", person_node1, thing_node)

    return con

some_names = ['Aarhus', 'Aeneid', 'Aldrich', 'Amelia', 'Anita', 'Ares', 'Atalanta', 'Babcock', 'Barnet', 'Beirut',
              'Berlioz', 'Birgit', 'Bonaventure', 'Brazzaville', 'Brunhilde', 'Buxton', 'Canberra', 'Cartesian',
              'Chang', 'Chou', 'Clifton', 'Conner', 'Creole', 'Dade', 'Daytona', 'Diana', 'Dooley', 'Dunlop', 'Edwin',
              'Ely', 'Eros', 'Ezra', 'Fiji', 'Francine', 'Fulton', 'Gaussian', 'Gilligan', 'Gorton', 'Guilford',
              'Hammond', 'Hausdorff', 'Heraclitus', 'Hinman', 'Horatio', 'Ibn', 'Irvin', 'Jamaica', 'Joanna',
              'Jugoslavia', 'Kathleen', 'Kikuyu', 'Koran', 'Langmuir', 'Leila', 'Lincoln', 'London', 'Luxembourg',
              'Magnuson', 'Marcy', 'Matisse', 'McElroy', 'Melcher', 'Midas', 'Moines', 'Morrill', 'Nagoya', 'Nero',
              'Nordstrom', 'Ojibwa', 'Osgood', 'Pareto', 'Peggy', 'Phipps', 'Poland', 'Prokofieff', 'Rae', 'Reub',
              'Rodriguez', 'Runge', 'Sancho', 'Schlesinger', 'Selkirk', 'Shenandoah', 'Sinbad', 'Sorenson', 'Steiner',
              'Sutton', 'Tasmania', 'Thomson', 'Transylvania', 'Ulster', 'Venusian', 'Waals', 'Weinberg', 'Wilkinson',
              'Wylie', 'Zen']

def init_friends_network(n_people=1000, n_connections=10000, db_uri=None):
    rng = random.Random(1)
    con = get_test_connection(db_uri)

    people_from = rng.choices(range(1,n_people+1), k=n_connections)
    people_to = rng.choices(range(1,n_people+1), k=n_connections)
    connections = list(zip(people_from, people_to))

    people_firstname = rng.choices(some_names, k=n_people)
    people_lastname = rng.choices(some_names, k=n_people)
    people_name = ["%s %s"%(f,l) for f,l in zip(people_firstname, people_lastname)]
    ages = [rng.randint(18,60) for i in range(n_people)]

    number_messages = [rng.randint(0,100) for i in range(n_connections)]

    con.add_nodes("person", n_people, [{'name': pn, 'age': page} for pn, page in zip(people_name,ages)])
    con.add_edges("likes", connections, [{'num_messages': n} for n in number_messages])
    return con

def assert_edge_connections(edges_list, pairs_list):
    """Assert that the list of Edge objects connects the nodes with IDs specified in the pairs_list"""
    for e, (from_id, to_id) in zip(edges_list, pairs_list):
        assert e.node_from_id==from_id and e.node_to_id==to_id