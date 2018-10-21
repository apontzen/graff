from sqlalchemy import Integer, ForeignKey
from sqlalchemy.orm import aliased

from graff import orm
from .base import BaseQuery


class EdgeQuery(BaseQuery):
    """Represents a query that returns nodes of a specific category or all categories"""

    _returns_edge = True
    # if True, a call to all() returns this edge (plus any other columns asked for)
    # if False, a call to all() does not return this node, only the other columns

    def __init__(self, graph_connection, category_=None):
        super(EdgeQuery, self).__init__(graph_connection)
        if category_:
            self._category = self._graph_connection.category_cache.get_id(category_)
        else:
            self._category = None

        if self._returns_edge:
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
        return NodeNamedPropertiesQuery(self, *args)

    def return_properties(self):
        return NodeAllPropertiesQuery(self)

    def return_this(self, *args):
        """Return a query that returns this node"""
        return PersistentNodeQuery(self)

    def filter(self, condition):
        """Return a new graph query that represents the old one filtered by a stated condition"""
        return NodeFilterNamedPropertiesQuery(self, condition)

    def follow(self, category=None):
        """Return a query that follows an edge to the next node.

        The edge may fall into a named category; or if None, all possible edges are followed."""
        return FollowQuery(self, category)