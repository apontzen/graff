from .orm import Base, Node, NodeProperty, Edge, EdgeProperty
from . import query
from . import category, value_mapping
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
        return query.node.NodeQuery(self, *args)

    def query_edge(self, *args):
        """Returns a query for edges, optionally of a given category"""
        return query.edge.EdgeQuery(self, *args)

    def add_node(self, category, properties=None):
        """Add a node of the specified category

        :param category: the category for the new nodes
        :type category: basestring

        :param properties - a property dictionary to populate the new nodes
        :type properties: dict

        :return: the new node
        :rtype: Node
        """
        new_node = Node()
        new_node.category_id = self.category_cache.get_existing_or_new_id(category)
        session = self.get_sqlalchemy_session()
        session.add(new_node)
        session.flush()

        if properties is not None:
            self._bulk_insert_properties(new_node.id, [properties], NodeProperty)

        session.commit()
        return new_node

    def add_nodes(self, category, number, properties=None):
        """Add multiple nodes of the specified category.

        :param category: the category for the new nodes
        :type category: basestring

        :param number: the number of objects to create

        :param properties - a list of property dictionaries to populate the new nodes; len(properties)=number
        :type properties: list[dict]

        """
        session = self.get_sqlalchemy_session()

        category_id = self.category_cache.get_existing_or_new_id(category)
        first_node_id = self._get_next_id(Node)
        session.bulk_insert_mappings(
            Node,
            [{'category_id': category_id}]*number
        )

        if properties is not None:
            if len(properties) != number:
                raise ValueError("Incorrect number of property dictionaries passed to add_nodes")
            self._bulk_insert_properties(first_node_id, properties, NodeProperty)
        session.commit()

    def _get_next_id(self, class_):
        session = self.get_sqlalchemy_session()
        first_id = session.query(class_.id).order_by(class_.id.desc()).first()
        if first_id is None:
            first_id = 1
        else:
            first_id = first_id[0] + 1
        return first_id

    def _bulk_insert_properties(self, first_parent_id, properties, class_):
        """Add properties for a sequential series of Nodes or Edges.

        :param first_parent_id: The ID of the first Node or Edge
        :param properties: a list of dictionaries for the properties to be inserted
        :type properties: list[dict]
        :param class_: the type of property to be added (NodeProperty or EdgeProperty)
        """
        if class_ is NodeProperty:
            id_name =  "node_id"
        elif class_ is EdgeProperty:
            id_name = "edge_id"
        else:
            raise ValueError("Unknown storage class passed to _bulk_insert_properties")

        session = self.get_sqlalchemy_session()
        property_object_mappings = []
        for i, props in enumerate(properties):
            for category, value in iteritems(props):
                category_id = self.category_cache.get_existing_or_new_id(category)
                dict_this_property = {id_name: first_parent_id + i, 'category_id': category_id}
                value_mapping.flexible_set_value(dict_this_property, value, attr=False, null_others=False)
                property_object_mappings.append(dict_this_property)
        session.bulk_insert_mappings(class_, property_object_mappings)

    def add_edge(self, category, node_from, node_to, properties=None):
        session = self.get_sqlalchemy_session()
        category_id = self.category_cache.get_existing_or_new_id(category)
        if isinstance(node_from,Node):
            node_from = node_from.id
        if isinstance(node_to, Node):
            node_to = node_to.id
        edge = Edge(category_id = category_id, node_from_id=node_from, node_to_id=node_to)
        session.add(edge)
        session.flush()
        if properties is not None:
            self._bulk_insert_properties(edge.id, [properties], EdgeProperty)
        session.commit()
        return edge

    def add_edges(self, category, mapping_pairs, properties=None):
        """Add multiple edges between the specified nodes.

        :param category: the category for the new edges
        :type category: basestring

        :param mapping_pairs: a list of pairs specifying all edges to be created; either as integer node IDs or node objects
        :type mapping_pairs: list[tuple[Any]]

        :return a list of the new edges
        :rtype list[Edge]
        """
        session = self.get_sqlalchemy_session()

        category_id = self.category_cache.get_existing_or_new_id(category)
        edges = []
        first_edge_id = self._get_next_id(Edge)
        for a,b in mapping_pairs:
            if isinstance(a, Node):
                a = a.id
            if isinstance(b, Node):
                b = b.id
            edges.append({'category_id': category_id, 'node_from_id': a, 'node_to_id': b})

        session.bulk_insert_mappings(Edge, edges)
        if properties is not None:
            if len(properties) != len(mapping_pairs):
                raise ValueError("Incorrect number of property dictionaries passed to add_edges")
            self._bulk_insert_properties(first_edge_id, properties, EdgeProperty)

        session.commit()

