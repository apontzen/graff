import graff.value_mapping as vm

def test_dict_mapping_int():
    d = {}
    vm.flexible_set_value(d, 1, attr=False, null_others=False)
    assert len(d)==1
    assert d['value_int']==1

def test_dict_mapping_float():
    d = {}
    vm.flexible_set_value(d, 1.0, attr=False, null_others=False)
    assert len(d)==1
    assert d['value_float']==1.0

def test_dict_mapping_str():
    d = {}
    vm.flexible_set_value(d, "hello", attr=False, null_others=False)
    assert len(d)==1
    assert d['value_str']=="hello"

class TestClass(object):
    pass

def test_obj_mapping_int():
    d = TestClass()
    vm.flexible_set_value(d, 1, attr=True, null_others=False)
    assert d.value_int==1
    assert not hasattr(d, "value_float")
    assert not hasattr(d, "value_str")

def test_obj_mapping_int_null_others():
    d = TestClass()
    vm.flexible_set_value(d, 1, attr=True, null_others=True)
    assert d.value_int == 1
    assert d.value_float is None
    assert d.value_str is None
