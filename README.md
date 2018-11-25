<img src="docs/graff.svg" width=200 title="graff">

_Graff_ is a graph database for python, implemented using [sqlalchemy](http://www.sqlalchemy.org) for 
compatibility with a wide range of database storage engines. It is also inspired by sqlalchemy in the way it
allows queries to be built generatively. But for most uses, the user does not need to have knowledge of
either SQL or sqlalchemy. 

_Graff_ evolved from the [tangos](pynbody.github.io/tangos) database project for cosmological simulations.

# Usage examples

Initialise a graph database in RAM:

```python
import graff
mydb = graff.Connection()
```

Initialise a graph database in the specified [mySQL](https://www.mysql.com) database:

```python
mydb = graff.Connection('mysql+pymysql://mysql_user:mysql_password@localhost/mysql_database_name')
```
(For more information see the [SQLAlchemy documentation](https://docs.sqlalchemy.org/en/latest/core/engines.html#mysql) 
around dialects and engines.)

Initialise a graph database in RAM and populate it with a random network of
people and friend relationships:
```python
import graff.testing
mydb = graff.testing.init_friends_network(n_people=10, n_connections=100)
```

Get the 'nodes' corresponding to all people:

```python
mydb.query_node("person").all()
```

Get the names of all people:

```python
mydb.query_node("person").return_property("name").all()
```

Count the number of friends-of-friends connections:
```python
mydb.query_node("person").follow("likes").follow("likes").count()
```

Get the names of all friends-of-friends, and print the first 100 pairs:
```python
fof = mydb.query_node("person").return_property("name").\
                                follow("likes").follow("likes").\
                                return_property("name").all()
for name_a, name_b in fof[:100]:
    print(name_b, "is a friend of a friend of", name_a)
```

Get all known properties of the first person in the database:
```python
mydb.query_node("person").return_properties().first()
```

Return the edge objects linking people:
```python
mydb.query_node("person").edge("likes").all()
```

Return a property from the edge as well as the nodes:
```python
results = mydb.query_node("person").return_property("name").\
                                    edge("likes").return_property("num_messages").\
                                    node().return_property("name").all()
                               
for name_a,num,name_b in results:
    print(name_a, "has sent", num, "messages to", name_b)

```