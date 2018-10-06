from . import orm, category, connection, condition
from sqlalchemy import Table, Column, Integer, ForeignKey, Float, sql
from sqlalchemy.orm import aliased
from six import iteritems
import random
import string
from six.moves import range
import copy

def add(category_, properties={}):
    new_node = orm.Node()
    new_node.category_id = category.get_existing_or_new_id(category_)
    session = connection.get_session()
    session.add(new_node)
    session.flush()

    property_objects = []
    for k, v in iteritems(properties):
        prop = orm.NodeProperty()
        prop.node_id = new_node.id
        prop.category_id = category.get_existing_or_new_id(k)
        prop.value = v
        property_objects.append(prop)

    session.add_all(property_objects)

    session.commit()
    return new_node

class BaseQuery(object):
    def __init__(self):
        session = connection.get_session()
        self._session = session
        self._connection = session.connection()
        self._temp_table = None

    def _create_temp_table(self):
        rstr = ''.join(random.choice(string.ascii_lowercase) for _ in range(4))

        temp_table = Table(
            'temporary_' + rstr,
            orm.Base.metadata,
            *self._get_temp_table_columns(),
            prefixes=['TEMPORARY']
        )

        # TODO: add suitable index to temp.temporary_+rstr

        self._temp_table = temp_table
        self._temp_table_name = "temporary_"+rstr
        self._temp_table.create(checkfirst=True, bind=self._connection)

    def _destroy_temp_table(self):
        self._temp_table.drop(checkfirst=True, bind=self._connection)
        self._temp_table = None

    def _filter_temp_table(self):
        pass

    def _get_temp_table_query(self):
        return self._session.query(orm.Node).select_from(self._temp_table).join(orm.Node)

    def all(self):
        with self:
            results = self._get_temp_table_query().all()
        return results

    def filter(self, condition):
        return NodeFilterNamedPropertiesQuery(self, condition)

    def temp_table(self):
        if self._temp_table is None:
            raise RuntimeError("Cannot get the temp_table without first entering the query")
        return self._temp_table

    def _populate_temp_table(self):
        raise NotImplementedError("_populate_temp_table needs to be implemented by a subclass")

    def _get_temp_table_columns(self):
        raise NotImplementedError("_get_temp_table_columns needs to be implemented by a subclass")

    def __enter__(self):
        self._create_temp_table()
        self._populate_temp_table()
        self._filter_temp_table()

    def __exit__(self, *args):
        self._destroy_temp_table()

    def follow(self, *args):
        """Return a query that follows an edge to the next node"""
        return FollowQuery(self, *args)


class NodeQuery(BaseQuery):
    def __init__(self, category_):
        if category_:
            self._category = category.get_id(category_)
        else:
            self._category = None
        super(NodeQuery, self).__init__()

    def _get_temp_table_columns(self):
        return [Column('id', Integer, primary_key=True), Column('node_id', Integer, ForeignKey('nodes.id'))]

    def _populate_temp_table(self):
        orm_query = self._session.query(orm.Node.id).filter_by(category_id=self._category)
        insert_statement = self._temp_table.insert().from_select(['node_id'], orm_query)
        self._connection.execute(insert_statement)

    def with_property(self, *args):
        """Return a query that returns properties"""
        if len(args)==0:
            return NodeAllPropertiesQuery(self)
        else:
            return NodeNamedPropertiesQuery(self, *args)

class FollowQuery(NodeQuery):
    def __init__(self, base, category_=None):
        super(FollowQuery, self).__init__(category_)
        self._base = base

    def _populate_temp_table(self):
        with self._base:
            prev_table = self._base.temp_table()
            query = self._session.query(orm.Edge.node_to_id)\
                .select_from(prev_table)\
                .join(orm.Edge,orm.Edge.node_from_id==prev_table.c.node_id)
            if self._category:
                query = query.filter(orm.Edge.category_id == self._category)
            insert_statement = self._temp_table.insert().from_select(["node_id"], query)
            self._connection.execute(insert_statement)


class NodeAllPropertiesQuery(BaseQuery):

    def _get_temp_table_columns(self):
        return  [Column('id', Integer, primary_key=True),
                 Column('node_id', Integer, ForeignKey('nodes.id')),
                 Column('property_id', Integer, ForeignKey('nodeproperties.id'))]

    def __init__(self, base):
        super(NodeAllPropertiesQuery, self).__init__()
        self._base = base

    def _populate_temp_table(self):
        with self._base:
            prev_table = self._base.temp_table()
            query = self._session.query(prev_table.c.node_id, orm.NodeProperty.id)\
                .select_from(prev_table)\
                .outerjoin(orm.NodeProperty,
                           (orm.NodeProperty.node_id==prev_table.c.node_id))

            insert_statement = self._temp_table.insert().from_select(["node_id", "property_id"], query)

            self._connection.execute(insert_statement)

    def _get_temp_table_query(self):
        return self._session.query(orm.Node,orm.NodeProperty.value).select_from(self._temp_table)\
            .outerjoin(orm.NodeProperty)\
            .join(orm.Node, orm.Node.id==self._temp_table.c.node_id)


class NodeNamedPropertiesQuery(BaseQuery):

    def _get_temp_table_columns(self):
        cols = [Column('id', Integer, primary_key=True),
                Column('node_id', Integer, ForeignKey('nodes.id'))]
        for i in range(len(self._categories)):
            cols.append(Column('property_id_%d'%i, Integer, ForeignKey('nodeproperties.id')))
        return cols

    def __init__(self, base, *categories):
        super(NodeNamedPropertiesQuery, self).__init__()
        self._category_names = categories
        self._categories = [category.get_id(c) for c in categories]
        self._base = base

    def _get_temp_table_column_mapping(self):
        return {name: sql.literal_column('property_value_%d'%i) for i, name in enumerate(self._category_names)}

    def _populate_temp_table(self):
        with self._base:
            prev_table = self._base.temp_table()
            aliases = []
            for cid in self._categories:
                aliases+=[aliased(orm.NodeProperty)]

            query = self._session.query(prev_table.c.node_id, *[a.id for a in aliases]) \
                .select_from(prev_table)

            for column, cid in zip(aliases, self._categories):
                query = query.outerjoin(column,
                           (column.node_id == prev_table.c.node_id) & (
                            column.category_id==cid))

            insert_cols = ["node_id"] + ["property_id_%d"%i for i in range(len(self._categories))]
            insert_statement = self._temp_table.insert().from_select(insert_cols, query)

            self._connection.execute(insert_statement)

    def _get_temp_table_query(self):

        aliases = []
        for cid in self._categories:
            aliases += [aliased(orm.NodeProperty)]

        q = self._session.query(orm.Node,*[a.value.label("property_value_%d"%i) for i,a in enumerate(aliases)]).select_from(self._temp_table)

        for i, alias in enumerate(aliases):
            q = q.outerjoin(alias, alias.id==getattr(self._temp_table.c, "property_id_%d"%i))

        q = q.join(orm.Node, orm.Node.id==self._temp_table.c.node_id)

        return q

class NodeFilterNamedPropertiesQuery(NodeNamedPropertiesQuery):
    def __init__(self, base, cond):
        self._condition = cond
        categories = cond.requires_properties()
        super(NodeFilterNamedPropertiesQuery, self).__init__(base, *categories)

    def _filter_temp_table(self):
        # in principle it would be neater to use a joined delete here, but sqlite doesn't support it
        # so we construct a subquery to figure out what to delete instead


        subq = self._session.query(self._temp_table.c.id)

        val_map = {}

        for i, category_name in enumerate(self._category_names):
            alias = aliased(orm.NodeProperty)
            subq = subq.outerjoin(alias, alias.id == getattr(self._temp_table.c, "property_id_%d" % i))
            val_map[category_name] = alias.value # this joined property should be used as the value in the condition we're evaluating

        self._condition.assign_sql_columns(val_map)
        delete_condition = ~(self._condition.to_sql()) # delete what we don't want to keep
        subq = subq.filter(delete_condition).subquery() # This subquery now identifies the IDs we don't want to keep

        delete_query = self._temp_table.delete().where(self._temp_table.c.id.in_(subq))

        self._connection.execute(delete_query)

def query(*args):
    return NodeQuery(*args)