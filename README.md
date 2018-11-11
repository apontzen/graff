<img src="docs/graff.svg" width=200 title="graff">

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

Initialise a graph database in RAM and populate it with a random network of
people and friend relationships
```python
mydb = graff.testing.init_friends_network(n_people=10, n_connections=100)
```

Get the 'nodes' corresponding to all people:

```python
nodes = mydb.query_node("person").all()
```

Get the names of all people:

```python
names = mydb.query_node("person").return_property("name").all()
```

Count the number of friends-of-friends connections:
```python
count = mydb.query_node("person").follow("likes").follow("likes").count()
```

Get the names of all friends-of-friends, and print the first 100 pairs:
```python
fof = mydb.query_node("person").return_property("name").\
                                follow("likes").follow("likes").\
                                return_property("name").all()
for name_a, name_b in fof[:100]:
    print(name_b, "is a friend of a friend of", name_a)
```
