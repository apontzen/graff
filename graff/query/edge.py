from sqlalchemy import Integer, ForeignKey
from sqlalchemy.orm import aliased

from .. import orm
from .base import *
from .node import GenericNodeQuery, NodeQueryFromUnderlyingQuery

class GenericEdgeQuery(BaseQuery):
    """Represents a query that returns nodes of a specific category or all categories"""
    _node_or_edge = 'edge'
    _node_or_edge_orm = orm.Edge
    _property_orm = orm.EdgeProperty

    def __init__(self, graph_connection):
        super(GenericEdgeQuery, self).__init__(graph_connection)


    @classmethod
    def _edge_query_callback(cls, column):
        alias = aliased(orm.Edge)
        return alias, alias, (alias.id==column)

    _user_query_callback = _edge_query_callback

    def return_property(self, *args):
        """Return a query that returns properties"""
        return EdgeNamedPropertiesQuery(self, *args)

    def return_properties(self):
        return EdgeAllPropertiesQuery(self)

    def return_this(self, *args):
        """Return a query that returns this node"""
        return PersistentEdgeQuery(self)

    def filter(self, condition):
        """Return a new graph query that represents the old one filtered by a stated condition"""
        return EdgeFilterNamedPropertiesQuery(self, condition)

    def node(self):
        """Return a query that follows an edge to the target node."""
        return TargetNodeQuery(self)

class EdgeQuery(QueryFromCategory, GenericEdgeQuery):
    pass

class TargetNodeQuery(NodeQueryFromUnderlyingQuery):
    def __init__(self, base):
        super(TargetNodeQuery, self).__init__(base)
        assert isinstance(base, GenericEdgeQuery)
        self._base = base

    def _get_populate_temp_table_statement(self):
        orm_query = self._session.query(orm.Edge.node_to_id, *self._copy_columns_source).\
            select_from(self._base.get_temp_table()).\
            join(orm.Edge, self._base._tt_current_location_id==orm.Edge.id)

        insert_statement = self.get_temp_table().insert().from_select([self._tt_current_location_id] + self._copy_columns_target, orm_query)

        return insert_statement

class EdgeQueryFromNodeQuery(GenericEdgeQuery, QueryFromUnderlyingQuery):
    def __init__(self, base, category_=None):
        assert isinstance(base, GenericNodeQuery)
        super(EdgeQueryFromNodeQuery, self).__init__(base)
        self._set_category(category_)

    def _get_populate_temp_table_statement(self):
        join_cond = self._base._tt_current_location_id == orm.Edge.node_from_id
        if self._category is not None:
            join_cond&= orm.Edge.category_id==self._category

        orm_query = self._session.query(orm.Edge.id, *self._copy_columns_source). \
            select_from(self._base.get_temp_table()). \
            join(orm.Edge, join_cond)

        insert_statement = self.get_temp_table().insert().from_select(
            [self._tt_current_location_id] + self._copy_columns_target, orm_query)

        return insert_statement

class EdgeQueryFromEdgeQuery(GenericEdgeQuery, QueryFromUnderlyingQuery):
    """Represents a query that returns edges based on a previous set of edges in an underlying 'base' query"""
    def __init__(self, base):
        assert isinstance(base, GenericEdgeQuery)
        super(EdgeQueryFromEdgeQuery, self).__init__(base)

class PersistentEdgeQuery(PersistentQuery, EdgeQueryFromEdgeQuery):
    _user_query_returns_self = False # don't also return the transient node column
    _persistent_query_callback = GenericEdgeQuery._edge_query_callback
    _persistent_postprocess_callback = None


class EdgeQueryWithValuesForInternalUse(QueryWithValuesForInternalUse, EdgeQueryFromEdgeQuery):
    pass

class EdgeNamedPropertiesQuery(NamedPropertiesQuery, EdgeQueryWithValuesForInternalUse):
    """Represents a query that returns the underlying edges, plus named properties of those edges.

    The properties are returned as columns, i.e. each named category generates a column that in turn has the
    value of the node's property. The row count is unchanged from the underlying query."""
    pass

class EdgeAllPropertiesQuery(AllPropertiesQuery, EdgeQueryFromEdgeQuery):
    pass

class EdgeFilterNamedPropertiesQuery(FilterNamedPropertiesQuery, EdgeQueryWithValuesForInternalUse):
    """Represents a query that returns the underlying edges, filtered by a condition that relies on named properties.

    Note that the properties are not returned to the user."""
    pass