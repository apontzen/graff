from .orm import Category

class RaiseException:
    pass


class CategoryCache(object):
    def __init__(self, sqlalchemy_session):
        self._name_ids = None
        self._session = sqlalchemy_session

    def get_id(self, name, default=RaiseException):

        if self._name_ids is None:
            id_and_name = self._session.query(Category.id, Category.name).all()
            self._name_ids = {t:i for i,t in id_and_name}

        if default is not RaiseException:
            return self._name_ids.get(name, default)
        else:
            return self._name_ids[name]

    def get_existing_or_new_id(self, name):

        id_ = self.get_id(name, None)

        if id_ is None:
            new_category = Category()
            new_category.name = name
            self._session.add(new_category)
            self._session.flush()
            id_ = self._name_ids[name] = new_category.id

        return id_