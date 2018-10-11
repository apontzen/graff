def flexible_set_value(object, value, attr=True, null_others=True):
    """Given an object, set either value_int, value_str, or value_float as appropriate.

    :param attr: if True, the attribute is set. If False, the dictionary value is set instead.
    :param null_others: if True, the remaining values are set to None. If False, they are ignored.
    """
    all_names = ['value_float', 'value_int', 'value_str']

    if isinstance(value, float):
        assigned_name = 'value_float'
    elif isinstance(value, int):
        assigned_name = 'value_int'
    elif isinstance(value, str):
        assigned_name = 'value_str'
    else:
        raise TypeError("Unable to assign this value to any of "+str(all_names))

    if attr:
        set_function = object.__setattr__
    else:
        set_function = object.__setitem__

    set_function(assigned_name, value)

    if null_others:
        for other_name in all_names:
            if other_name!=assigned_name:
                set_function(other_name, None)

