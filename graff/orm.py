from sqlalchemy import Column, Integer, Float, String, ForeignKey, Index, func, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, composite
from sqlalchemy.ext.hybrid import hybrid_property
from . import value_mapping, config

Base = declarative_base()


class Category(Base):
    __tablename__ = "categories"

    id = Column(Integer, primary_key=True)
    name = Column(String(config.category_max_length), unique=True)

    def __repr__(self):
        return "<Category %r>"%self.name

class SupportsCastToDict(object):
    def __iter__(self):
        for property_item in self.properties:
            yield property_item.category.name, property_item.value

class Node(Base, SupportsCastToDict):
    __tablename__ = "nodes"

    id = Column(Integer, primary_key=True)
    category_id = Column(Integer, ForeignKey("categories.id"))
    category = relationship(Category)

    def __repr__(self):
        if self.category is not None:
            return "<Node id=%d category=%r>"%(self.id, self.category.name)
        else:
            return "<Node id=%d category=???>"


class Edge(Base, SupportsCastToDict):
    __tablename__ = "edges"

    id = Column(Integer, primary_key=True)
    node_from_id = Column(Integer, ForeignKey("nodes.id"))
    node_to_id = Column(Integer, ForeignKey("nodes.id"))
    category_id = Column(Integer, ForeignKey("categories.id"))

    node_from = relationship(Node, foreign_keys=[node_from_id])
    node_to = relationship(Node, foreign_keys=[node_to_id])
    category = relationship(Category)

    def __repr__(self):
        return "<Edge (%d -> %d) category=%r>"%(self.node_from_id, self.node_to_id, self.category.name)




class FlexibleValueStorage(object):
    @classmethod
    def py_coalesce(cls, *args):
        for a in args:
            if a is not None:
                return a

    @hybrid_property
    def value(self):
        return self.py_coalesce(self.value_int, self.value_float, self.value_str)

    @value.expression
    def value(cls):
        return func.coalesce(cls.value_int, cls.value_float, cls.value_str)

    @value.setter
    def value(self, val):
        value_mapping.flexible_set_value(self, val)

class NodeProperty(Base, FlexibleValueStorage):
    __tablename__ = "nodeproperties"

    id = Column(Integer, primary_key=True)
    node_id = Column(Integer, ForeignKey("nodes.id"), nullable=False)
    node = relationship(Node, backref='properties', innerjoin=True)
    category_id = Column(Integer, ForeignKey("categories.id"), nullable=False)
    category = relationship(Category, innerjoin=True)

    value_int = Column(Integer)
    value_float = Column(Float)
    value_str = Column(Text)


class EdgeProperty(Base, FlexibleValueStorage):
    __tablename__ = "edgeproperties"

    id = Column(Integer, primary_key=True)
    edge_id = Column(Integer, ForeignKey("edges.id"))
    edge = relationship(Edge, backref='properties', innerjoin=True)
    category_id = Column(Integer, ForeignKey("categories.id"))
    category = relationship(Category)

    value_int = Column(Integer)
    value_float = Column(Float)
    value_str = Column(Text)


Index("edges_node_from_index", Edge.__table__.c.node_from_id)
Index("edges_node_to_index", Edge.__table__.c.node_to_id)
Index("node_index", Node.__table__.c.id)
Index("edge_index", Edge.__table__.c.id)
Index("nodeproperties_node_index", NodeProperty.__table__.c.node_id)
Index("edgeproperties_edge_index", EdgeProperty.__table__.c.edge_id)
