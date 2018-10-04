from sqlalchemy import Column, Integer, Float, String, ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, composite

from .flexible_value import FlexibleValue

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


class Edge(Base):
    __tablename__ = "edges"

    id = Column(Integer, primary_key=True)
    node_from_id = Column(Integer, ForeignKey("nodes.id"))
    node_to_id = Column(Integer, ForeignKey("nodes.id"))
    category_id = Column(Integer, ForeignKey("categories.id"))

    node_from = relationship(Node, foreign_keys=[node_from_id])
    node_to = relationship(Node, foreign_keys=[node_to_id])
    category = relationship(Category)


class NodeProperty(Base):
    __tablename__ = "nodeproperties"

    id = Column(Integer, primary_key=True)
    node_id = Column(Integer, ForeignKey("nodes.id"))
    node = relationship(Node)
    category_id = Column(Integer, ForeignKey("categories.id"))
    category = relationship(Category)

    value_int = Column(Integer)
    value_float = Column(Float)
    value_str = Column(String)

    value = composite(FlexibleValue, value_int, value_float, value_str)

    def __init__(self):
        self.value = FlexibleValue()



class EdgeProperty(Base):
    __tablename__ = "edgeproperties"

    id = Column(Integer, primary_key=True)
    edge_id = Column(Integer, ForeignKey("edges.id"))
    edge = relationship(Edge)
    category_id = Column(Integer, ForeignKey("categories.id"))
    category = relationship(Category)

    value_int = Column(Integer)
    value_float = Column(Float)
    value_str = Column(String)

    value = composite(FlexibleValue, value_int, value_float, value_str)



Index("edges_node_from_index", Edge.__table__.c.node_from_id)
Index("edges_node_to_index", Edge.__table__.c.node_to_id)
Index("nodeproperties_node_index", NodeProperty.__table__.c.node_id)
Index("edgeproperties_edge_index", EdgeProperty.__table__.c.edge_id)
