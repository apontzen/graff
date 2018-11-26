from sqlalchemy import Column, Integer, Float, String, ForeignKey, Index, func, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, composite
from sqlalchemy.orm.properties import CompositeProperty
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




class FlexibleValue(object):
    @classmethod
    def py_coalesce(cls, *args):
        for a in args:
            if a is not None:
                return a

    def __init__(self, value_int, value_float, value_str):
        self.value_int = value_int
        self.value_float = value_float
        self.value_str = value_str

    def __composite_values__(self):
        return self.value_int, self.value_float, self.value_str

    @property
    def value(self):
        return self.py_coalesce(self.value_int, self.value_float, self.value_str)

    def __repr__(self):
        return self.value

    def __str__(self):
        return str(self.value)

    def __int__(self):
        return self.value_int

    def __float__(self):
        return self.value_float

    def __eq__(self, other):
        return self.value==other

    def __hash__(self):
        return hash(self.value)

    @value.setter
    def value(self, val):
        value_mapping.flexible_set_value(self, val)

class FlexibleComparator(CompositeProperty.Comparator):
    def __gt__(self, other):
        if hasattr(other, "__composite_values__"):
            return func.coalesce(*[a>b for a,b in zip(self.__clause_element__().clauses, other.__composite_values__())])
        else:
            return func.coalesce(*[a > other for a in self.__clause_element__().clauses])


class NodeProperty(Base):
    __tablename__ = "nodeproperties"

    id = Column(Integer, primary_key=True)
    node_id = Column(Integer, ForeignKey("nodes.id"), nullable=False)
    node = relationship(Node, backref='properties', innerjoin=True)
    category_id = Column(Integer, ForeignKey("categories.id"), nullable=False)
    category = relationship(Category, innerjoin=True)

    value_int = Column(Integer)
    value_float = Column(Float)
    value_str = Column(Text)

    value = composite(FlexibleValue, value_int, value_float, value_str, comparator_factory=FlexibleComparator)


class EdgeProperty(Base):
    __tablename__ = "edgeproperties"

    id = Column(Integer, primary_key=True)
    edge_id = Column(Integer, ForeignKey("edges.id"))
    edge = relationship(Edge, backref='properties', innerjoin=True)
    category_id = Column(Integer, ForeignKey("categories.id"))
    category = relationship(Category)

    value_int = Column(Integer)
    value_float = Column(Float)
    value_str = Column(Text)

    value = composite(FlexibleValue, value_int, value_float, value_str, comparator_factory=FlexibleComparator)


Index("edges_node_from_index", Edge.__table__.c.node_from_id)
Index("edges_node_to_index", Edge.__table__.c.node_to_id)
Index("node_index", Node.__table__.c.id)
Index("edge_index", Edge.__table__.c.id)
Index("nodeproperties_node_index", NodeProperty.__table__.c.node_id)
Index("edgeproperties_edge_index", EdgeProperty.__table__.c.edge_id)
