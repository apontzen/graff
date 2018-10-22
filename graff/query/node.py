from sqlalchemy import Integer, ForeignKey, sql
from sqlalchemy.orm import aliased, joinedload

from graff import orm
from .base import BaseQuery, QueryFromUnderlyingQuery, PersistentQuery


class NodeQuery(BaseQuery):
    """Represents a query that returns nodes of a specific category or all categories"""

    _user_query_returns_self = True
    # if True, a call to all() returns this node (plus any other columns asked for)
    # if False, a call to all() does not return this node, only the other columns

    def __init__(self, graph_connection, category_=None):
        super(NodeQuery, self).__init__(graph_connection)
        if category_:
            self._category = self._graph_connection.category_cache.get_id(category_)
        else:
            self._category = None

        if self._user_query_returns_self:
            self._tt_current_location_id = self._temp_table_state.add_column("node_id", Integer, ForeignKey('nodes.id'),
                                                                     query_callback = self._node_query_callback)
        else:
            self._tt_current_location_id = self._temp_table_state.add_column("noreturn_node_id", Integer, ForeignKey('nodes.id'),
                                                                     query_callback = self._null_query_callback)

    @staticmethod
    def _null_query_callback(column):
        return None, None, None

    @staticmethod
    def _node_query_callback(column):
        alias = aliased(orm.Node)
        return alias, alias, (alias.id==column)

    def _get_populate_temp_table_statement(self):
        orm_query = self._session.query(orm.Node.id).filter_by(category_id=self._category)
        insert_statement = self.get_temp_table().insert().from_select([self._tt_current_location_id], orm_query)
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

        The edge may fall into a named category; or if None, all possible edges are followed.

        Note that the q.follow(category) is equivalent to, but more efficient than, q.edge(category).node()"""
        return FollowQuery(self, category)

    def edge(self, category=None):
        """Return a query that returns all edges from this node.

        The edges may fall into a named category; or if None, all possible edges are returned."""
        from . import edge
        return edge.EdgeQueryFromNodeQuery(self, category)


class NodeQueryFromUnderlyingQuery(NodeQuery, QueryFromUnderlyingQuery):
    pass


class NodeQueryFromNodeQuery(NodeQueryFromUnderlyingQuery):
    """Represents a query that returns nodes based on a previous set of nodes in an underlying 'base' query"""
    def __init__(self, base, category_=None):
        assert isinstance(base, NodeQuery)
        super(NodeQueryFromNodeQuery, self).__init__(base, category_)

    def _get_populate_temp_table_statement(self):
        orm_query = self._session.query(self._base._tt_current_location_id, *self._copy_columns_source)

        if self._category:
            orm_query = orm_query.filter_by(category_id=self._category)
        insert_statement = self.get_temp_table().insert().from_select([self._tt_current_location_id] + self._copy_columns_target, orm_query)

        return insert_statement


class PersistentNodeQuery(PersistentQuery, NodeQueryFromNodeQuery):
    _user_query_returns_self = False # don't also return the transient node column
    _persistent_query_callback = staticmethod(NodeQueryFromNodeQuery._node_query_callback)
    _persistent_postprocess_callback = None


class FollowQuery(NodeQueryFromNodeQuery):
    """Represents a query that returns nodes linked by edges to the previous nodes.

    The edges may fall into a particular category, or if no category is specified all edges are followed."""

    def _get_populate_temp_table_statement(self):
        prev_table = self._base.get_temp_table()
        query = self._session.query(orm.Edge.node_to_id, *self._copy_columns_source)\
            .select_from(prev_table)\
            .outerjoin(orm.Edge, orm.Edge.node_from_id == self._base._tt_current_location_id)
        # outer join to capture multiple edges; but will later filter to remove NULL entries

        if self._category:
            query = query.filter(orm.Edge.category_id == self._category)
        insert_statement = self.get_temp_table().insert().from_select([self._tt_current_location_id] + self._copy_columns_target, query)
        return insert_statement

    def _filter_temp_table(self):
        # Remove NULL entries generated by outer join above
        tt = self.get_temp_table()
        self._connection.execute(tt.delete().where(self._tt_current_location_id == None))


class NodeAllPropertiesQuery(PersistentNodeQuery):
    """Represents a query that returns the underlying nodes, plus all their properties.

    The properties are returned as rows, i.e. each row contains the node and one of its properties.
    Thus the column count is increased by one, but the row count is increased by a number depending on how
    many properties each node has."""


    @staticmethod
    def _persistent_query_callback(column):
        alias = aliased(orm.Node)
        return alias, alias, (alias.id == column), joinedload(alias.properties).joinedload(orm.NodeProperty.category)

    @staticmethod
    def _persistent_postprocess_callback(results, column_id):
        new_results = []
        for row in results:
            property_dict = dict(row[column_id])
            new_results.append(row[:column_id] + (property_dict,) + row[column_id+1:])
        return new_results


class NodeQueryWithValuesForInternalUse(NodeQueryFromNodeQuery):
    """Represents a query that returns the underlying nodes and also internally obtains values of the named properties.
    This is a base class for alternative possible uses for the properties. It does not directly expose the results
    to the user, and therefore is not recommended for direct use. NodeNamedPropertiesQuery is the user-exposed class
    that actually returns the values of the property, whereas NodeFilterNamedPropertiesQuery instead uses the
    values to perform a filter on the returned results.
    """
    _column_base = "noreturn_nodeproperty_id"

    _property_query_callback = staticmethod(NodeQuery._null_query_callback)

    def __init__(self, base, *categories):
        super(NodeQueryWithValuesForInternalUse, self).__init__(base)
        self._category_names = categories
        self._categories = [self._graph_connection.category_cache.get_id(c) for c in categories]
        self._tt_column_mapping = {}
        self._tt_columns = []
        for n in self._category_names:
            new_col = self._temp_table_state.add_column_with_unique_name(self._column_base,
                                                                         Integer, ForeignKey('nodeproperties.id'),
                                                                         query_callback=self._property_query_callback)
            self._tt_column_mapping[n] = new_col
            self._tt_columns.append(new_col)

    def _get_temp_table_column_mapping(self):
        return self._tt_column_mapping

    def _get_populate_temp_table_statement(self):
        prev_table = self._base.get_temp_table()
        underlying_tt_current_location_id = self._base._tt_current_location_id
        aliases = []
        for this_category_id in self._categories:
            aliases+=[aliased(orm.NodeProperty)]

        query = self._session.query(underlying_tt_current_location_id, *(self._copy_columns_source + [a.id for a in aliases])) \
            .select_from(prev_table)

        for this_property_alias, this_category_id in zip(aliases, self._categories):
            query = query.outerjoin(this_property_alias,
                       (this_property_alias.node_id == underlying_tt_current_location_id) & (
                        this_property_alias.category_id==this_category_id))

        insert_cols = [self._tt_current_location_id] + self._copy_columns_target + self._tt_columns
        insert_statement = self.get_temp_table().insert().from_select(insert_cols, query)

        return insert_statement


class NodeNamedPropertiesQuery(NodeQueryWithValuesForInternalUse):
    """Represents a query that returns the underlying nodes, plus named properties of those nodes.

    The properties are returned as columns, i.e. each named category generates a column that in turn has the
    value of the node's property. The row count is unchanged from the underlying query."""
    _user_query_returns_self = False
    _column_base = "nodeproperty_id"

    @staticmethod
    def _property_query_callback(column):
        alias = aliased(orm.NodeProperty)
        return alias.value, alias, alias.id == column

    def __getitem__(self, item):
        from .. import condition
        sql_column = self._get_temp_table_column_mapping()[item]
        sql_column_in_future_table = sql.literal_column(sql_column.name)
        return condition.BoundProperty(item, sql_column_in_future_table)

    def _get_temp_table_columns_to_carry_forward(self):
        return super(NodeNamedPropertiesQuery, self)._get_temp_table_columns_to_carry_forward() + self._tt_columns


class NodeFilterNamedPropertiesQuery(NodeQueryWithValuesForInternalUse):
    """Represents a query that returns the underlying nodes, filtered by a condition that relies on named properties.

    Note that the properties are not returned to the user."""

    _user_query_returns_self = True

    def __init__(self, base, cond):
        self._condition = cond
        categories = cond.get_unresolved_property_names()
        super(NodeFilterNamedPropertiesQuery, self).__init__(base, *categories)

    def _filter_temp_table(self):
        # in principle it would be neater to use a joined delete here, but sqlite doesn't support it
        # so we construct a subquery to figure out what to delete instead

        tt = self.get_temp_table()

        subq = self._session.query(tt.c.id)

        value_map = {}

        for col, category_name in zip(self._tt_columns, self._category_names):
            alias = aliased(orm.NodeProperty)
            subq = subq.outerjoin(alias, alias.id == col)
            value_map[category_name] = alias.value # this joined property should be used as the value in the condition we're evaluating

        for id_column in self._condition.get_resolved_property_id_columns():
            alias = aliased(orm.NodeProperty)
            subq = subq.outerjoin(alias, alias.id == id_column)
            value_map[id_column] = alias.value  # this joined property should be used as the value in the condition we're evaluating

        self._condition.assign_sql_columns(value_map)

        delete_condition = ~(self._condition.to_sql()) # delete what we don't want to keep
        subq = subq.filter(delete_condition).subquery() # This subquery now identifies the IDs we don't want to keep

        delete_query = tt.delete().where(tt.c.id.in_(subq))

        self._connection.execute(delete_query)