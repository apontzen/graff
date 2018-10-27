from sqlalchemy import Integer, ForeignKey
from sqlalchemy.orm import aliased

from .. import orm
from .base import *
from .node import GenericNodeQuery, NodeQueryFromUnderlyingQuery

class GenericEdgeQuery(BaseQuery):
    """Represents a query that returns nodes of a specific category or all categories"""

    def __init__(self, graph_connection):
        super(GenericEdgeQuery, self).__init__(graph_connection)

        if self._user_query_returns_self:
            self._tt_current_location_id = self._temp_table_state.add_column("edge_id", Integer, ForeignKey('edges.id'),
                                                                     query_callback = self._edge_query_callback)
        else:
            self._tt_current_location_id = self._temp_table_state.add_column("noreturn_edge_id", Integer, ForeignKey('edges.id'),
                                                                     query_callback = self._null_query_callback)

    @classmethod
    def _edge_query_callback(cls, column):
        alias = aliased(orm.Edge)
        return alias, alias, (alias.id==column)

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
        return NodeFilterEdgePropertiesQuery(self, condition)

    def node(self):
        """Return a query that follows an edge to the target node."""
        return TargetNodeQuery(self)

class EdgeQuery(GenericEdgeQuery):
    def __init__(self, graph_connection, category_=None):
        super(EdgeQuery, self).__init__(graph_connection)
        self._set_category(category_)

    def _get_populate_temp_table_statement(self):
        orm_query = self._session.query(orm.Edge.id).filter_by(category_id=self._category)
        insert_statement = self.get_temp_table().insert().from_select([self._tt_current_location_id], orm_query)
        return insert_statement

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
    _node_or_edge = 'edge'


class EdgeQueryWithValuesForInternalUse(QueryWithValuesForInternalUse, EdgeQueryFromEdgeQuery):
    _node_or_edge = 'edge'
    _node_or_edge_orm = orm.Edge
    _property_orm = orm.EdgeProperty

class EdgeNamedPropertiesQuery(NamedPropertiesQuery, EdgeQueryWithValuesForInternalUse):
    """Represents a query that returns the underlying edges, plus named properties of those edges.

    The properties are returned as columns, i.e. each named category generates a column that in turn has the
    value of the node's property. The row count is unchanged from the underlying query."""
    pass