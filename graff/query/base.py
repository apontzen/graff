from sqlalchemy.orm import aliased

from .. import orm
from ..temptable import TempTableState


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

    @staticmethod
    def _null_query_callback(column):
        return None, None, None