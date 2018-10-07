import re
from . import orm
from sqlalchemy import Table, Column, Integer, ForeignKey, sql
from sqlalchemy.orm import Session

class TempTableStateError(RuntimeError):
    """Raised when a manipulation requires the temp table to exist in the database but it does not, or vice versa."""

class TempTableState(object):
    """Represents a temp table both before and during its existence.

    Columns can be added before the creation of the temp table, and they can either be explicitly named or the class
    can create new unique names if required."""
    def __init__(self):
        self._columns = [Column('id', Integer, primary_key=True)]
        self._active = False

    def get_columns(self):
        """Return all Column objects in the schema for this temp table"""
        return self._columns

    def get_column_names(self):
        """Return the names of all columns in the schema"""
        return [str(x.name) for x in self._columns]

    def add_column(self, *args):
        """Add a column to the schema.

        If a single argument of type sqlalchemy.Column is provided, it is added literally to the schema.

        Otherwise the arguments are interpreted as arguments to the sqlalchemy.Column constructor"""
        self._assert_not_active()
        if len(args)==1 and isinstance(args[0], Column):
            new_column = args[0]
        else:
            new_column = Column(*args)
        self._columns.append(new_column)
        return new_column

    def add_column_with_unique_name(self, name_root, *args):
        """Add a column to the schema; the name starts with name_root and is unique relative to existing columns.

        The remaining arguments are passed to the sqlalchemy.Column constructor"""
        self._assert_not_active()
        names = self.get_column_names()
        new_name = self._generate_unique_name(name_root, names)

        return self.add_column(new_name, *args)

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

        temp_table = Table(
            self._generate_unique_name("temptable",orm.Base.metadata.tables.keys()),
            orm.Base.metadata,
            *self.get_columns(),
            prefixes=['TEMPORARY']
        )

        # TODO: add suitable index to the table just generated

        self._temp_table = temp_table
        self._temp_table.create(checkfirst=True, bind=self._connection)

        self._active = True

    def get_table(self):
        """Get the temporary table. Will throw an error if the table has not yet been created."""
        self._assert_active()
        return self._temp_table

    def destroy(self):
        """Destroy the temporary table and return the schema to being mutable."""
        self._assert_active()
        self._temp_table.drop(checkfirst=True, bind=self._connection)
        # TODO: drop index
        orm.Base.metadata.remove(self._temp_table)

        self._temp_table = None

        self._active = False



