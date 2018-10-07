from .orm import Base, Node, NodeProperty, Edge
from . import node
from . import category
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from six import iteritems
from six.moves import range

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
        """Returns the SQLAlchemy Session object that queries will be based upon"""
        return self._internal_session

    def query_node(self, *args):
        """Returns a query for nodes, optionally of a given category"""
        return node.NodeQuery(self,*args)

    def add_node(self, category, properties={}):
        """Add a node of the specified category

        :param category: the category for the new nodes
        :type category: basestring

        :param properties - a property dictionary to populate the new nodes
        :type properties: dict

        :return: the new node
        :rtype: Node
        """
        return self.add_nodes(category, 1, [properties])[0]

    def add_nodes(self, category, number, properties=None):
        """Add multiple nodes of the specified category.

        :param category: the category for the new nodes
        :type category: basestring

        :param number: the number of objects to create

        :param properties - a list of property dictionaries to populate the new nodes; len(properties)=number
        :type properties: list[dict]

        :return: A list of the new nodes
        :rtype: list[Node]
        """
        session = self.get_sqlalchemy_session()

        category_id = self.category_cache.get_existing_or_new_id(category)
        new_nodes = [Node(category_id=category_id) for i in range(number)]
        session.add_all(new_nodes)
        session.flush()

        if properties is not None:
            if len(properties)!=number:
                raise ValueError("Incorrect number of property dictionaries passed to add_nodes")
            property_objects = []
            for node, props in zip(new_nodes, properties):
                for category, value in iteritems(props):
                    category_id = self.category_cache.get_existing_or_new_id(category)
                    property_objects.append(NodeProperty(node_id = node.id, category_id=category_id, value=value))
            session.add_all(property_objects)
        session.commit()
        return new_nodes

    def add_edge(self, category, node_from, node_to):
        return self.add_edges(category, [(node_from, node_to)])[0]

    def add_edges(self, category, mapping_pairs):
        """Add multiple edges between the specified nodes.

        :param category: the category for the new edges
        :type category: basestring

        :param mapping_pairs: a list of pairs specifying all edges to be created
        :type mapping_pairs: list[tuple[Node]]

        :return a list of the new edges
        :rtype list[Edge]
        """
        session = self.get_sqlalchemy_session()

        category_id = self.category_cache.get_existing_or_new_id(category)
        edges = []
        for a,b in mapping_pairs:
            edges.append(Edge(node_from=a, node_to=b, category_id=category_id))
        session.add_all(edges)
        session.commit()
        return edges

