# graff
_Graff_ is a graph database for python, implemented using [sqlalchemy](http://www.sqlalchemy.org) for 
compatibility with a wide range of database storage engines. It is also inspired by sqlalchemy in the way it
allows queries to be built generatively. But for most uses, the user does not need to have knowledge of
either SQL or sqlalchemy. 

_Graff_ evolved from the [tangos](pynbody.github.io/tangos) database project for cosmological simulations.

# Usage examples

Initialise a graph database in RAM:

```python
mydb= graff.Connection()
```

Get all nodes of a specified type:

```python
nodes = mydb.query_nodes("person").all()
```

Count the number of friends-of-friends connections:
```python
count = mydb.query_nodes("person").follow("friend").follow("friend").count()
```

Print all friends-of-friends:
```python
fof = mydb.query_nodes("person").with_property("name").follow("friend").follow("friend").with_property("name").all()
for name_a, name_b in fof:
    print(name_b, "is a friend of a friend of", name_a)
```
