from .orm import Base, Node, NodeProperty, Edge
from . import node
from . import category
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from six import iteritems

class Connection(object):
    def __init__(self, db_uri="", verbose=False, timeout=30):
        if '//' not in db_uri:
            db_uri = 'sqlite:///' + db_uri

        _engine = create_engine(db_uri, echo=verbose,
                                isolation_level='READ UNCOMMITTED', connect_args={'timeout': timeout})

        self._SessionClass = sessionmaker(bind=_engine)
        self._internal_session = self._SessionClass()
        self.category_cache = category.CategoryCache(self.get_sqlalchemy_session())
        Base.metadata.create_all(_engine)

    def get_sqlalchemy_session(self):
        return self._internal_session

    def query_node(self, *args):
        return node.NodeQuery(self,*args)

    def add_node(self, category_, properties={}):
        new_node = Node()
        new_node.category_id = self.category_cache.get_existing_or_new_id(category_)
        session = self.get_sqlalchemy_session()
        session.add(new_node)
        session.flush()

        property_objects = []
        for k, v in iteritems(properties):
            prop = NodeProperty()
            prop.node_id = new_node.id
            prop.category_id = self.category_cache.get_existing_or_new_id(k)
            prop.value = v
            property_objects.append(prop)

        session.add_all(property_objects)

        session.commit()
        return new_node

    def add_edge(self, node_from, node_to, category_):
        new_edge = Edge()
        new_edge.category_id = self.category_cache.get_existing_or_new_id(category_)
        new_edge.node_from = node_from
        new_edge.node_to = node_to
        session = self.get_sqlalchemy_session()
        session.add(new_edge)
        session.commit()