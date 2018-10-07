from . import orm, category, connection
from sqlalchemy import Table, Column, Integer, ForeignKey, sql
from sqlalchemy.orm import aliased
from six import iteritems
import copy
from .temptable import TempTableState


class QueryStructureError(RuntimeError):
    pass

class BaseQuery(object):
    """The base class for all graph queries.

    Queries do not perform any actions until one actually requests results using the all() method, gets the first
    result using first(), or counts them using count().

    For more advanced usage, it is helpful to understand what happens when all(), first() or count() is called.

    First the query is "entered" as a context; this involves creating a temp table in the SQL layer which contains the
    results. These results can then be retrieved by querying against the temp table. A suitable query is
    returned by _get_temp_table_query().

    Thus, for example, nodes = q.all() for a basic node query q should be equivalent to:

    with q:
        tt = q.temp_table()
        nodes = session.query(orm.Node).select_from(tt).join(orm.Node

    Once the query context exits, the temp table is destroyed. In other words, any manipulation of the temp table within
    SQL must be performed within the context.
    """

    def __init__(self, graph_connection):
        self._graph_connection = graph_connection
        self._session = graph_connection.get_sqlalchemy_session()
        self._connection = self._session.connection()
        self._temp_table_state = TempTableState()

    def _get_populate_temp_table_statement(self):
        """Get the SQL statement to insert rows into the temporary table for this query.

        Called immediately after the temporary table has been created, meaning the query context has just been entered.

        The SQL returned by this statement is called immediately. The reason that it returns the query rather than
        executing it itself is so that child classes can modify the generated statement (rather than have to
        re-implement it in its entirety)."""
        raise NotImplementedError("_populate_temp_table needs to be implemented by a subclass")

    def _filter_temp_table(self):
        """Apply any filters to the temporary table for this query.

        Called when entering the query context, just after creating and populating the temporary table."""
        pass

    def _get_temp_table_query(self):
        """Get the correct SQL query against the temp table to return appropriate results from this graph query."""

        tt = self._temp_table_state

        assert tt.get_columns()[0].name=="id"

        join_conditions = []
        query_on = [tt.get_table().c.id] # must include this column to prevent sqlalchemy de-duping returned nodes
        join_tables = []

        for col in tt.get_columns()[1:]:
            if col.name.startswith("noreturn"):
                pass
            elif col.name.startswith("node_id"):
                alias = aliased(orm.Node)
                query_on.append(alias)
                join_conditions.append(alias.id==col)
                join_tables.append(alias)
            elif col.name.startswith("nodeproperty_id"):
                alias =aliased(orm.NodeProperty)
                query_on.append(alias.value)
                join_conditions.append(alias.id==col)
                join_tables.append(alias)
            else:
                raise ValueError("Don't know how to return results from a temptable column named %s"%(col.name))

        q = self._session.query(*query_on).select_from(tt.get_table())

        for table, condition in zip(join_tables,join_conditions):
            q = q.outerjoin(table, condition)
            # use outer join so that null IDs translate to null in output, rather than disappearing

        return q

    def all(self):
        """Construct and retrieve all results from this graph query"""
        with self:
            results = self._get_temp_table_query().all()
        results = [r[1:] for r in results] # remove temp-table ID column
        return results

    def count(self):
        """Constructs the query and counts the number of rows in the result"""
        with self:
            return self._session.query(self.get_temp_table()).count()

    def first(self):
        """Constructs the query and returns the first row in the result"""
        with self:
            result = self._get_temp_table_query().first()
        if result is None:
            return None
        else:
            return result[1:] # remove temp-table ID column

    def filter(self, condition):
        """Return a new graph query that represents the old one filtered by a stated condition"""
        return NodeFilterNamedPropertiesQuery(self, condition)

    def get_temp_table(self):
        """Return the SQLAlchemy Table for the temp table associated with this query.

        This method will fail unless you have first entered the query."""
        return self._temp_table_state.get_table()

    def follow(self, category=None):
        """Return a query that follows an edge to the next node.

        The edge may fall into a named category; or if None, all possible edges are followed."""
        return FollowQuery(self, category)

    def _get_temp_table_columns(self):
        """Return a list of columns to be created in the temp table"""
        raise NotImplementedError("_get_temp_table_columns needs to be implemented by a subclass")

    def _get_temp_table_columns_to_carry_forward(self):
        """Return a list of columns in the temp table that should be propagated into any chained queries.

        For example, this allows the results of with_property(...) to propagate along the query chain"""
        return []

    def __getitem__(self, item):
        """Return a reference to a named property in this query, suitable for use in a filter condition"""
        raise QueryStructureError("This query does not have any named properties to reference")

    def __enter__(self):
        self._temp_table_state.create(self._session)
        self._connection.execute(self._get_populate_temp_table_statement())
        self._filter_temp_table()

    def __exit__(self, *args):
        return self._temp_table_state.destroy()



class NodeQuery(BaseQuery):
    """Represents a query that returns nodes of a specific category or all categories"""
    def __init__(self, graph_connection, category_=None):
        super(NodeQuery, self).__init__(graph_connection)
        if category_:
            self._category = self._graph_connection.category_cache.get_id(category_)
        else:
            self._category = None

        self._temp_table_state.add_column("node_id", Integer, ForeignKey('nodes.id'))


    def _get_populate_temp_table_statement(self):
        orm_query = self._session.query(orm.Node.id).filter_by(category_id=self._category)
        insert_statement = self.get_temp_table().insert().from_select(['node_id'], orm_query)
        return insert_statement

    def with_property(self, *args):
        """Return a query that returns properties"""
        if len(args)==0:
            return NodeAllPropertiesQuery(self)
        else:
            return NodeNamedPropertiesQuery(self, *args)

    def with_this(self, *args):
        """Return a query that returns this node"""
        return PersistentNodeQuery(self)


class NodeQueryFromNodeQuery(NodeQuery):
    """Represents a query that returns nodes based on a previous set of nodes in an underlying 'base' query"""
    def __init__(self, base, category_=None):
        super(NodeQueryFromNodeQuery, self).__init__(base._graph_connection, category_)
        self._base = base
        self._copy_columns_source = []
        self._copy_columns_target = []
        for col in base._get_temp_table_columns_to_carry_forward():
            self._copy_columns_source.append(col)
            self._copy_columns_target.append(self._temp_table_state.add_column(copy.copy(col)))

    def __enter__(self):
        with self._base:
            super(NodeQueryFromNodeQuery, self).__enter__()

    def _get_temp_table_columns_to_carry_forward(self):
        return self._copy_columns_target

    def _get_populate_temp_table_statement(self):
        orm_query = self._session.query(self._base._temp_table_state.get_columns()[1], *self._copy_columns_source)

        if self._category:
            orm_query = orm_query.filter_by(category_id=self._category)
        insert_statement = self.get_temp_table().insert().from_select(['node_id']+self._copy_columns_target, orm_query)

        return insert_statement

class PersistentNodeQuery(NodeQueryFromNodeQuery):
    def __init__(self, base):
        super(PersistentNodeQuery, self).__init__(base)
        self._copy_columns_source.append(base._temp_table_state.get_columns()[1].label("node_id_persist_source"))
        self._copy_columns_target.append(self._temp_table_state.add_column_with_unique_name("node_id_persistent", Integer, ForeignKey("nodes.id")))



class FollowQuery(NodeQueryFromNodeQuery):
    """Represents a query that returns nodes linked by edges to the previous nodes.

    The edges may fall into a particular category, or if no category is specified all edges are followed."""

    def _get_populate_temp_table_statement(self):
        prev_table = self._base.get_temp_table()
        query = self._session.query(orm.Edge.node_to_id, *self._copy_columns_source)\
            .select_from(prev_table)\
            .outerjoin(orm.Edge,orm.Edge.node_from_id==prev_table.c.node_id)
        # outer join to capture multiple edges; but will later filter to remove NULL entries

        if self._category:
            query = query.filter(orm.Edge.category_id == self._category)
        insert_statement = self.get_temp_table().insert().from_select(["node_id"]+self._copy_columns_target, query)
        return insert_statement

    def _filter_temp_table(self):
        # Remove NULL entries generated by outer join above
        tt = self.get_temp_table()
        self._connection.execute(tt.delete().where(tt.c.node_id==None))


class NodeAllPropertiesQuery(NodeQueryFromNodeQuery):
    """Represents a query that returns the underlying nodes, plus all their properties.

    The properties are returned as rows, i.e. each row contains the node and one of its properties.
    Thus the column count is increased by one, but the row count is increased by a number depending on how
    many properties each node has."""

    def __init__(self, base):
        super(NodeAllPropertiesQuery, self).__init__(base)
        self._tt_property_column = \
            self._temp_table_state.add_column_with_unique_name("nodeproperty_id", Integer, ForeignKey('nodeproperties.id'))

    def _get_populate_temp_table_statement(self):
        prev_table = self._base.get_temp_table()
        query = self._session.query(prev_table.c.node_id, orm.NodeProperty.id, *self._copy_columns_source)\
            .select_from(prev_table)\
            .outerjoin(orm.NodeProperty,
                       (orm.NodeProperty.node_id==prev_table.c.node_id))

        insert_statement = self.get_temp_table().insert().from_select(["node_id", self._tt_property_column]
                                                                      +self._copy_columns_target, query)
        return insert_statement

    def _get_temp_table_query(self):
        return self._session.query(orm.Node,orm.NodeProperty.value).select_from(self.get_temp_table())\
            .outerjoin(orm.NodeProperty)\
            .join(orm.Node, orm.Node.id == self.get_temp_table().c.node_id)

    def _get_temp_table_columns_to_carry_forward(self):
        return super(NodeAllPropertiesQuery, self)._get_temp_table_columns_to_carry_forward() + [self._tt_property_column]


class NodeQueryWithValuesForInternalUse(NodeQueryFromNodeQuery):
    """Represents a query that returns the underlying nodes and also internally obtains values of the named properties.

    This is a base class for alternative possible uses for the properties. It does not directly expose the results
    to the user, and therefore is not recommended for direct use. NodeNamedPropertiesQuery is the user-exposed class
    that actually returns the values of the property, whereas NodeFilterNamedPropertiesQuery instead uses the
    values to perform a filter on the returned results.
    """
    _column_base = "noreturn_nodeproperty_id"

    def __init__(self, base, *categories):
        super(NodeQueryWithValuesForInternalUse, self).__init__(base)
        self._category_names = categories
        self._categories = [self._graph_connection.category_cache.get_id(c) for c in categories]
        self._tt_column_mapping = {}
        self._tt_columns = []
        for n in self._category_names:
            new_col = self._temp_table_state.add_column_with_unique_name(self._column_base,
                                                                         Integer, ForeignKey('nodeproperties.id'))
            self._tt_column_mapping[n] = new_col
            self._tt_columns.append(new_col)

    def _get_temp_table_column_mapping(self):
        return self._tt_column_mapping

    def _get_populate_temp_table_statement(self):
        prev_table = self._base.get_temp_table()
        aliases = []
        for cid in self._categories:
            aliases+=[aliased(orm.NodeProperty)]

        query = self._session.query(prev_table.c.node_id, *(self._copy_columns_source + [a.id for a in aliases])) \
            .select_from(prev_table)

        for column, cid in zip(aliases, self._categories):
            query = query.outerjoin(column,
                       (column.node_id == prev_table.c.node_id) & (
                        column.category_id==cid))

        insert_cols = ["node_id"] + self._copy_columns_target + self._tt_columns
        insert_statement = self.get_temp_table().insert().from_select(insert_cols, query)

        return insert_statement


class NodeNamedPropertiesQuery(NodeQueryWithValuesForInternalUse):
    """Represents a query that returns the underlying nodes, plus named properties of those nodes.

    The properties are returned as columns, i.e. each named category generates a column that in turn has the
    value of the node's property. The row count is unchanged from the underlying query."""

    _column_base = "nodeproperty_id"

    def __getitem__(self, item):
        from . import condition
        sql_column = self._get_temp_table_column_mapping()[item]
        sql_column_in_future_table = sql.literal_column(sql_column.name)
        return condition.BoundProperty(item, sql_column_in_future_table)

    def _get_temp_table_columns_to_carry_forward(self):
        return super(NodeNamedPropertiesQuery, self)._get_temp_table_columns_to_carry_forward() + self._tt_columns


class NodeFilterNamedPropertiesQuery(NodeQueryWithValuesForInternalUse):
    """Represents a query that returns the underlying nodes, filtered by a condition that relies on named properties.

    Note that the properties are not returned to the user."""
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
