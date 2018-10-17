import re
from . import orm
from sqlalchemy import Table, Column, Integer, Index, ForeignKey, sql
from sqlalchemy.orm import Session

class TempTableStateError(RuntimeError):
    """Raised when a manipulation requires the temp table to exist in the database but it does not, or vice versa."""

class TempTableState(object):
    """Represents a temp table both before and during its existence.

    Columns can be added before the creation of the temp table, and they can either be explicitly named or the class
    can create new unique names if required."""
    def __init__(self):
        self._columns = [Column('id', Integer, primary_key=True)]
        self._columns_query_callback = [self._default_column_callback]
        self._active = False

    @staticmethod
    def _default_column_callback(column):
        return column, None, None

    def get_columns(self):
        """Return all Column objects in the schema for this temp table"""
        return self._columns

    def get_callback_for_column(self, column):
        return self._columns_query_callback[self._columns.index(column)]

    def get_column(self, column_number):
        """Return a specific column based on the column ordering (starting at zero)"""
        return self._columns[column_number]

    def get_column_names(self):
        """Return the names of all columns in the schema"""
        return [str(x.name) for x in self._columns]

    def add_column(self, *args, **kwargs):
        """Add a column to the schema.

        If a single argument of type sqlalchemy.Column is provided, it is added literally to the schema.

        Otherwise the arguments are interpreted as arguments to the sqlalchemy.Column constructor. Additionally:

        :arg query_callback: A function called at query time, to present the data in the temp table to a user.
          It is passed this column and the existing query, and must modify the query in such a way as to add
          a column for the data the user actually wants to see. If query_callback is not specified, the literal
          column is added.

        """

        self._assert_not_active()

        query_callback = kwargs.pop('query_callback', self._default_column_callback)

        if len(args)==1 and isinstance(args[0], Column):
            new_column = args[0]
        else:
            new_column = Column(*args)
        self._columns.append(new_column)
        self._columns_query_callback.append(query_callback)
        return new_column

    def add_column_with_unique_name(self, name_root, *args, **kwargs):
        """Add a column to the schema; the name starts with name_root and is unique relative to existing columns.

        The remaining arguments and keyword arguments are passed to add_column"""
        self._assert_not_active()
        names = self.get_column_names()
        new_name = self._generate_unique_name(name_root, names)

        return self.add_column(new_name, *args, **kwargs)

    @classmethod
    def _generate_unique_name(cls, name_root, existing_names):
        existing_names = list(filter(lambda x: re.match(re.escape(name_root) + "_[0-9]*$", x),
                                     existing_names))
        if len(existing_names)>0:
            max_current_id = max([int(re.match(re.escape(name_root)+"_([0-9]*)$", x).group(1)) for x in existing_names])
        else:
            max_current_id = -1

        new_name = name_root + "_%d" % (max_current_id+1)
        assert new_name not in existing_names
        return new_name

    def _assert_not_active(self):
        if self._active:
            raise TempTableStateError("Cannot perform this operation on the temp table while it is active in the database")

    def _assert_active(self):
        if not self._active:
            raise TempTableStateError("Cannot perform this operation until the temp table is active in the database")

    def create(self, sqlalchemy_session):
        """Create the temporary table. The schema becomes immutable until destroy() is called"""
        self._assert_not_active()
        self._connection = sqlalchemy_session.connection()
        self._session = sqlalchemy_session

        temp_table = Table(
            self._generate_unique_name("temptable",orm.Base.metadata.tables.keys()),
            orm.Base.metadata,
            *self.get_columns(),
            prefixes=['TEMPORARY']
        )


        self._temp_table = temp_table
        self._table_index = Index('temp.index_' + temp_table.name, self.get_columns()[1])
        self._temp_table.create(checkfirst=True, bind=self._connection)


        self._active = True

    def get_table(self):
        """Get the temporary table. Will throw an error if the table has not yet been created."""
        self._assert_active()
        return self._temp_table

    def get_query(self):
        """Return the sqlalchemy query for recovering user data from this table.

        Will throw an error if the table has not yet been created."""
        self._assert_active()
        query_entities = []
        join_entities = []
        join_conditions = []
        for col, callback in zip(self._columns, self._columns_query_callback):
            query_entity, join_entity, join_condition = callback(col)
            if query_entity is not None:
                query_entities.append(query_entity)
                if join_entity is not None:
                    assert join_condition is not None
                    join_entities.append(join_entity)
                    join_conditions.append(join_condition)
            else:
                assert join_entity is None
                assert join_condition is None

        q = self._session.query(*query_entities).select_from(self.get_table())

        for table, condition in zip(join_entities, join_conditions):
            q = q.outerjoin(table, condition)
            # use outer join so that null IDs translate to null in output, rather than disappearing

        return q

    def destroy(self):
        """Destroy the temporary table and return the schema to being mutable."""
        self._assert_active()
        self._table_index.drop(bind=self._connection)
        self._temp_table.drop(checkfirst=True, bind=self._connection)

        orm.Base.metadata.remove(self._temp_table)

        self._temp_table = None

        self._active = False



