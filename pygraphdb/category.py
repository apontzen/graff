from .orm import Category
from .connection import get_session

class RaiseException:
    pass


_name_ids = None
def get_id(name, session=None, default=RaiseException):
    global _name_ids

    if session is None:
        session = get_session()

    if _name_ids is None:
        id_and_name = session.query(Category.id, Category.name).all()
        _name_ids = {t:i for i,t in id_and_name}

    if default is not RaiseException:
        return _name_ids.get(name, default)
    else:
        return _name_ids[name]

def get_existing_or_new_id(name, session=None):
    if session is None:
        session = get_session()

    id_ = get_id(name, session, None)

    if id_ is None:
        new_category = Category()
        new_category.name = name
        session.add(new_category)
        session.flush()
        id_ = _name_ids[name] = new_category.id

    return id_