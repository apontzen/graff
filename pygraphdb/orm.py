from sqlalchemy import Column, Integer, Float, String, ForeignKey, Index, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, composite
from sqlalchemy.ext.hybrid import hybrid_property

Base = declarative_base()


class Category(Base):
    __tablename__ = "categories"

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)

    def __repr__(self):
        return "<Category %r>"%self.text


class Node(Base):
    __tablename__ = "nodes"

    id = Column(Integer, primary_key=True)
    category_id = Column(Integer, ForeignKey("categories.id"))
    category = relationship(Category)

    def __repr__(self):
        if self.category is not None:
            return "<Node id=%d category=%r>"%(self.id, self.category.name)
        else:
            return "<Node id=%d category=???>"


class Edge(Base):
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
    def py_coalesce(*args):
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
        if isinstance(val, float):
            self.value_float = val
            self.value_int = self.value_str = None
        elif isinstance(val, int):
            self.value_int = val
            self.value_float = self.value_str = None
        elif isinstance(val, str):
            self.value_str = val
            self.value_float = self.value_int = None

class NodeProperty(Base, FlexibleValueStorage):
    __tablename__ = "nodeproperties"

    id = Column(Integer, primary_key=True)
    node_id = Column(Integer, ForeignKey("nodes.id"))
    node = relationship(Node)
    category_id = Column(Integer, ForeignKey("categories.id"))
    category = relationship(Category)

    value_int = Column(Integer)
    value_float = Column(Float)
    value_str = Column(String)


class EdgeProperty(Base, FlexibleValueStorage):
    __tablename__ = "edgeproperties"

    id = Column(Integer, primary_key=True)
    edge_id = Column(Integer, ForeignKey("edges.id"))
    edge = relationship(Edge)
    category_id = Column(Integer, ForeignKey("categories.id"))
    category = relationship(Category)

    value_int = Column(Integer)
    value_float = Column(Float)
    value_str = Column(String)


Index("edges_node_from_index", Edge.__table__.c.node_from_id)
Index("edges_node_to_index", Edge.__table__.c.node_to_id)
Index("nodeproperties_node_index", NodeProperty.__table__.c.node_id)
Index("edgeproperties_edge_index", EdgeProperty.__table__.c.edge_id)
