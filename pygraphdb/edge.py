from . import orm, category, connection

def add(node_from, node_to, category_):
    new_edge = orm.Edge()
    new_edge.category_id = category.get_existing_or_new_id(category_)
    new_edge.node_from = node_from
    new_edge.node_to = node_to
    session = connection.get_session()
    session.add(new_edge)
    session.commit()