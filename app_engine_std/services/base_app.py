#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from python_easy_json import JSONObject


class BaseAppEndpoint:
    """
    Base class for all end-points (jobs, tasks, pub/sub, ...) in all apps.
    """
    def _get_annot_cls(self, key: str):
        """
        For the given property name of self, return the defined annotation class if available, otherwise
        return a stock JSONObject class.
        :param key: Key in self.__annotations__.
        :return: Annotation class or JSONObject class
        """
        cls_ = JSONObject
        if hasattr(self, '__annotations__') and key in self.__annotations__:
            cls_ = self.__annotations__[key]
            # See if this annotation is a list of objects, if so, get the first
            # available object type in the list.
            if hasattr(cls_, '_name') and cls_._name == 'List':
                cls_ = cls_.__args__[0]
        return cls_
