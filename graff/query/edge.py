from sqlalchemy import Integer, ForeignKey
from sqlalchemy.orm import aliased

from .. import orm
from .base import BaseQuery, QueryFromUnderlyingQuery
from .node import NodeQuery, NodeQueryFromUnderlyingQuery

class EdgeQuery(BaseQuery):
    """Represents a query that returns nodes of a specific category or all categories"""

    def __init__(self, graph_connection, category_=None):
        super(EdgeQuery, self).__init__(graph_connection)
        if category_:
            self._category = self._graph_connection.category_cache.get_id(category_)
        else:
            self._category = None

        if self._user_query_returns_self:
            self._tt_edge_column = self._temp_table_state.add_column("edge_id", Integer, ForeignKey('nodes.id'),
                                                                     query_callback = self._edge_query_callback)
        else:
            self._tt_edge_column = self._temp_table_state.add_column("noreturn_edge_id", Integer, ForeignKey('nodes.id'),
                                                                     query_callback = self._null_query_callback)

    @staticmethod
    def _edge_query_callback(column):
        alias = aliased(orm.Edge)
        return alias, alias, (alias.id==column)

    def _get_populate_temp_table_statement(self):
        orm_query = self._session.query(orm.Edge.id).filter_by(category_id=self._category)
        insert_statement = self.get_temp_table().insert().from_select([self._tt_edge_column], orm_query)
        return insert_statement

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


class TargetNodeQuery(NodeQueryFromUnderlyingQuery):
    def __init__(self, base, category_=None):
        super(TargetNodeQuery, self).__init__(base)
        assert isinstance(base, EdgeQuery)
        self._base = base
        self._carry_forward_temp_table_columns(base)

    def _get_populate_temp_table_statement(self):
        orm_query = self._session.query(orm.Edge.node_to_id, *self._copy_columns_source).\
            select_from(self._base.get_temp_table()).\
            join(orm.Edge, self._base._tt_edge_column==orm.Edge.id)

        insert_statement = self.get_temp_table().insert().from_select([self._tt_current_location_id] + self._copy_columns_target, orm_query)

        return insert_statement

class EdgeQueryFromNodeQuery(EdgeQuery, QueryFromUnderlyingQuery):
    def __init__(self, base, category_=None):
        assert isinstance(base, NodeQuery)
        super(EdgeQueryFromNodeQuery, self).__init__(base, category_)

    def _get_populate_temp_table_statement(self):
        join_cond = self._base._tt_current_location_id == orm.Edge.node_from_id
        if self._category is not None:
            join_cond&= orm.Edge.category_id==self._category

        orm_query = self._session.query(orm.Edge.id, *self._copy_columns_source). \
            select_from(self._base.get_temp_table()). \
            join(orm.Edge, join_cond)

        insert_statement = self.get_temp_table().insert().from_select(
            [self._tt_edge_column] + self._copy_columns_target, orm_query)

        return insert_statement

