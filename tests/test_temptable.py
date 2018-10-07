from pygraphdb import temptable
from sqlalchemy import Integer

def test_column_adding():
    schema = temptable.TempTableState()
    assert schema.get_column_names()==["id"]

    schema.add_column_with_unique_name("root", Integer)
    assert schema.get_column_names()[-1]=="root_0"

    schema.add_column_with_unique_name("root", Integer)
    assert len(schema.get_columns())==3
    assert schema.get_column_names()[-1] == "root_1"

    schema.add_column_with_unique_name("differentroot", Integer)
    assert len(schema.get_columns()) == 4
    assert schema.get_column_names()[-1] == "differentroot_0"


def test_column_adding_with_existing():
    schema = temptable.TempTableState()

    schema.add_column("root_7", Integer)
    schema.add_column_with_unique_name("root", Integer)
    schema.add_column_with_unique_name("root", Integer)

    schema.add_column("differentroot_sausage", Integer)
    schema.add_column_with_unique_name("differentroot", Integer)
    schema.add_column_with_unique_name("differentroot", Integer)

    assert schema.get_column_names()==["id", "root_7","root_8","root_9", "differentroot_sausage",
                                       "differentroot_0", "differentroot_1"]

