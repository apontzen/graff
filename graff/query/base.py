import copy
from sqlalchemy import Integer, ForeignKey, sql
from sqlalchemy.orm import aliased, joinedload

from ..temptable import TempTableState
from .. import orm


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

    _node_or_edge = None  # child class to set this to 'node' or 'edge'
    _node_or_edge_orm = None # must be set to either orm.Node or orm.Edge by child class
    _property_orm = None  # must be set to either orm.NodeProperty or orm.EdgeProperty by child class

    _user_query_returns_self = True
    # if True, a call to all() returns this node or edge (plus any other columns asked for)
    # if False, a call to all() does not return this node or edge, only the other columns

    def __init__(self, graph_connection):
        self._graph_connection = graph_connection
        self._session = graph_connection.get_sqlalchemy_session()
        self._connection = self._session.connection()
        self._category = None
        self._temp_table_state = TempTableState()
        self._copy_columns_target = []
        if self._user_query_returns_self:
            self._tt_current_location_id = self._temp_table_state.add_column(self._node_or_edge+"_id", Integer,
                                                                             #ForeignKey(self._node_or_edge+'s.id'),
                                                                     query_callback = self._user_query_callback,
                                                                     keep_at_end = True)
        else:
            self._tt_current_location_id = self._temp_table_state.add_column("noreturn_node_id", Integer,
                                                                             #ForeignKey('nodes.id'),
                                                                     query_callback = self._null_query_callback,
                                                                     keep_at_end =True)


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
        return self._temp_table_state.get_query()

    @classmethod
    def _reformat_results_row(cls, results):
        if results is None:
            return None
        elif len(results)==2:
            return results[1]
        elif len(results)>2:
            return results[1:]
        else:
            raise ValueError("SQL query returned row with too few columns (%d)"%len(results))

    def all(self):
        """Construct and retrieve all results from this graph query"""
        with self:
            results = self._get_temp_table_query().all()

        results = self._temp_table_state.postprocess_results(results)

        results = list(map(self._reformat_results_row, results))
        return results

    def count(self):
        """Constructs the query and counts the number of rows in the result"""
        with self:
            return self._session.query(self.get_temp_table()).count()

    def first(self):
        """Constructs the query and returns the first row in the result"""
        with self:
            result = self._get_temp_table_query().first()
        result = self._temp_table_state.postprocess_results([result])[0]
        return self._reformat_results_row(result)


    def get_temp_table(self):
        """Return the SQLAlchemy Table for the temp table associated with this query.

        This method will fail unless you have first entered the query."""
        return self._temp_table_state.get_table()

    def _get_temp_table_columns(self):
        """Return a list of columns to be created in the temp table"""
        raise NotImplementedError("_get_temp_table_columns needs to be implemented by a subclass")

    def _get_temp_table_columns_to_carry_forward(self):
        """Return a list of columns in the temp table that should be propagated into any chained queries.

        For example, this allows the results of return_property(...) to propagate along the query chain"""
        return self._copy_columns_target

    def __getitem__(self, item):
        """Return a reference to a named property in this query, suitable for use in a filter condition"""
        raise QueryStructureError("This query does not have any named properties to reference")

    def __enter__(self):
        self._temp_table_state.create(self._session)
        self._connection.execute(self._get_populate_temp_table_statement())
        self._filter_temp_table()

    def __exit__(self, *args):
        return self._temp_table_state.destroy()

    @staticmethod
    def _null_query_callback(column):
        return None, None, None

    def _set_category(self, category_):
        if category_:
            self._category = self._graph_connection.category_cache.get_id(category_)
        else:
            self._category = None

class QueryFromCategory(BaseQuery):
    """Represents a query that returns nodes/edges of a given category"""

    def __init__(self, graph_connection, category_=None):
        super(QueryFromCategory, self).__init__(graph_connection)
        self._set_category(category_)

    def _get_populate_temp_table_statement(self):
        orm_query = self._session.query(self._node_or_edge_orm.id).filter_by(category_id=self._category)
        insert_statement = self.get_temp_table().insert().from_select([self._tt_current_location_id], orm_query)
        return insert_statement

class QueryFromUnderlyingQuery(BaseQuery):
    """Represents a query that returns nodes based on a previous set of nodes in an underlying 'base' query"""

    def __init__(self, base):
        super(QueryFromUnderlyingQuery, self).__init__(base._graph_connection)
        self._carry_forward_temp_table_columns(base)
        self._base = base

    def __enter__(self):
        with self._base:
            super(QueryFromUnderlyingQuery, self).__enter__()

    def _get_populate_temp_table_statement(self):
        orm_query = self._session.query(self._base._tt_current_location_id, *self._copy_columns_source)

        if self._category:
            orm_query = orm_query.filter_by(category_id=self._category)
        insert_statement = self.get_temp_table().insert().from_select(
            [self._tt_current_location_id] + self._copy_columns_target, orm_query)

        return insert_statement

    def _carry_forward_temp_table_columns(self, base):
        self._base = base
        self._copy_columns_source = []
        self._copy_columns_target = []
        for col in base._get_temp_table_columns_to_carry_forward():
            self._copy_columns_source.append(col)
            self._copy_columns_target.append(
                self._temp_table_state.add_column(copy.copy(col), query_callback=base._temp_table_state.get_query_callback_for_column(col),
                                                                  postprocess_callback=base._temp_table_state.get_postprocess_callback_for_column(col)
                                                  )
                                             )

class PersistentQuery(QueryFromUnderlyingQuery):
    """Represents a query where a result will be carried forward into any derived queries"""

    _user_query_returns_self = False # the persistent column will be returned, so don't also return this

    @classmethod
    def _persistent_query_callback(cls, column):
        raise RuntimeError("This callback must be implemented in a subclass")

    @classmethod
    def _persistent_postprocess_callback(cls, results, column_id):
        raise RuntimeError("This callback must be implemented in a subclass")

    def __init__(self, base):
        super(PersistentQuery, self).__init__(base)
        self._copy_columns_source.append(base._temp_table_state.get_columns()[-1].label(self._node_or_edge+"_id_persist_source"))
        self._copy_columns_target.append(self._temp_table_state.add_column_with_unique_name(self._node_or_edge+"_id_persistent",
                                                                                            Integer, #ForeignKey(self._node_or_edge+"s.id"),
                                                                                            query_callback=self._persistent_query_callback,
                                                                                            postprocess_callback=self._persistent_postprocess_callback))

class AllPropertiesQuery(PersistentQuery):
    """Represents a query that returns the underlying nodes, plus all their properties.

    The properties are returned as rows, i.e. each row contains the node and one of its properties.
    Thus the column count is increased by one, but the row count is increased by a number depending on how
    many properties each node has."""

    @classmethod
    def _persistent_query_callback(cls, column):
        alias = aliased(cls._node_or_edge_orm)
        return alias, alias, (alias.id == column), joinedload(alias.properties).joinedload(cls._property_orm.category)

    @classmethod
    def _persistent_postprocess_callback(cls, results, column_id):
        new_results = []
        for row in results:
            property_dict = dict(row[column_id])
            new_results.append(row[:column_id] + (property_dict,) + row[column_id+1:])
        return new_results

class QueryWithValuesForInternalUse(QueryFromUnderlyingQuery):
    """Represents a query that returns the underlying query and also internally obtains values of the named properties.
    """
    _column_base = "property_id"
    _property_query_callback = staticmethod(BaseQuery._null_query_callback)

    def __init__(self, base, *categories):
        super(QueryWithValuesForInternalUse, self).__init__(base)
        self._category_names = categories
        self._categories = [self._graph_connection.category_cache.get_id(c) for c in categories]
        self._tt_column_mapping = {}
        self._tt_columns = []
        for n in self._category_names:
            new_col = self._temp_table_state.add_column_with_unique_name(self._column_base,
                                                                         Integer, #ForeignKey(self._property_orm.id),
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
            aliases+=[aliased(self._property_orm)]

        query = self._session.query(underlying_tt_current_location_id, *(self._copy_columns_source + [a.id for a in aliases])) \
            .select_from(prev_table)

        for this_property_alias, this_category_id in zip(aliases, self._categories):
            property_alias_node_or_edge_id = getattr(this_property_alias, self._node_or_edge+"_id")
            query = query.outerjoin(this_property_alias,
                       (property_alias_node_or_edge_id == underlying_tt_current_location_id) & (
                        this_property_alias.category_id==this_category_id))

        insert_cols = [self._tt_current_location_id] + self._copy_columns_target + self._tt_columns
        insert_statement = self.get_temp_table().insert().from_select(insert_cols, query)

        return insert_statement


class NamedPropertiesQuery(QueryWithValuesForInternalUse):
    """Represents a query that returns the underlying nodes, plus named properties of those nodes.

    The properties are returned as columns, i.e. each named category generates a column that in turn has the
    value of the node's property. The row count is unchanged from the underlying query."""
    _user_query_returns_self = False

    @classmethod
    def _property_query_callback(cls, column):
        alias = aliased(cls._property_orm)
        return alias.value, alias, alias.id == column

    def __getitem__(self, item):
        from .. import condition
        sql_column = self._get_temp_table_column_mapping()[item]
        sql_column_in_future_table = sql.literal_column(sql_column.name)
        return condition.BoundProperty(item, sql_column_in_future_table)

    def _get_temp_table_columns_to_carry_forward(self):
        return super(NamedPropertiesQuery, self)._get_temp_table_columns_to_carry_forward() + self._tt_columns


class FilterNamedPropertiesQuery(QueryWithValuesForInternalUse):
    """Represents a query that returns the underlying nodes, filtered by a condition that relies on named properties.

    Note that the properties are not returned to the user."""

    _user_query_returns_self = True

    def __init__(self, base, cond):
        self._condition = cond
        categories = cond.get_unresolved_property_names()
        super(FilterNamedPropertiesQuery, self).__init__(base, *categories)

    def _filter_temp_table(self):
        # in principle it would be neater to use a joined delete here, but sqlite doesn't support it
        # so we construct a subquery to figure out what to delete instead

        tt = self.get_temp_table()

        subq = self._session.query(tt.c.id)

        value_map = {}

        for col, category_name in zip(self._tt_columns, self._category_names):
            alias = aliased(self._property_orm)
            subq = subq.outerjoin(alias, alias.id == col)
            value_map[category_name] = alias.value # this joined property should be used as the value in the condition we're evaluating

        for id_column in self._condition.get_resolved_property_id_columns():
            alias = aliased(self._property_orm)
            subq = subq.outerjoin(alias, alias.id == id_column)
            value_map[id_column] = alias.value  # this joined property should be used as the value in the condition we're evaluating

        self._condition.assign_sql_columns(value_map)

        delete_condition = ~(self._condition.to_sql()) # delete what we don't want to keep
        subq = subq.filter(delete_condition).subquery() # This subquery now identifies the IDs we don't want to keep

        delete_query = tt.delete().where(tt.c.id.in_(subq))

        self._connection.execute(delete_query)