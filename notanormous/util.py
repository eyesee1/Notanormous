# -*- coding: utf-8 -*-

import time

__all__ = ['make_embeddable', 'clean_output', 'cached_property']


___debone_toplevel___ = ('_id', 'metadata')
___debone_data___ = ('_classname', '_version')


# stolen from https://github.com/mitsuhiko/werkzeug/blob/master/werkzeug/utils.py#L30

class _Nothing(object):

    def __repr__(self):
        return 'no value'

    def __reduce__(self):
        return '_nothing'

_nothing = _Nothing()



# cached_property:
# © 2011 Christopher Arndt, MIT License
# taken from http://wiki.python.org/moin/PythonDecoratorLibrary

class cached_property(object):
    """Decorator for read-only properties evaluated only once within TTL period.

    It can be used to created a cached property like this::

        import random

        # the class containing the property must be a new-style class
        class MyClass(object):
            # create property whose value is cached for ten minutes
            @cached_property(ttl=600)
            def randint(self):
                # will only be evaluated every 10 min. at maximum.
                return random.randint(0, 100)

    The value is cached  in the '_cache' attribute of the object instance that
    has the property getter method wrapped by this decorator. The '_cache'
    attribute value is a dictionary which has a key for every property of the
    object which is wrapped by this decorator. Each entry in the cache is
    created only when the property is accessed for the first time and is a
    two-element tuple with the last computed property value and the last time
    it was updated in seconds since the epoch.

    The default time-to-live (TTL) is 300 seconds (5 minutes). Set the TTL to
    zero for the cached value to never expire.

    To expire a cached property value manually just do::
    
        del instance._cache[<property name>]

    """
    def __init__(self, ttl=300):
        self.ttl = ttl

    def __call__(self, fget, doc=None):
        self.fget = fget
        self.__doc__ = doc or fget.__doc__
        self.__name__ = fget.__name__
        self.__module__ = fget.__module__
        return self

    def __get__(self, inst, owner):
        now = time.time()
        try:
            value, last_update = inst._cache[self.__name__]
            if self.ttl > 0 and now - last_update > self.ttl:
                raise AttributeError
        except (KeyError, AttributeError):
            value = self.fget(inst)
            try:
                cache = inst._cache
            except AttributeError:
                cache = inst._cache = {}
            cache[self.__name__] = (value, now)
        return value
    
    def __get__(self, obj, type=None):
        if obj is None:
            return self
        value = obj.__dict__.get(self.__name__, _nothing)
        if value is _nothing:
            try:
                value = self.func(obj)
            except Exception, msg:
                ex = "Cached property {0} of Document {1} could not complete its function because: {2}".format(
                    self.__name__, obj.__class__.__name__, msg)
                print ex
                raise Exception(ex)
            obj.__dict__[self.__name__] = value
        return value



def make_embeddable(document, *args):
    """
    Makes a clean embeddable version of a document, stripping out:
    * id
    * Notanormous' metadata
    * any field called `metadata`
    * optionally, any fields you tell it to delete. Supply them as args.
    """
    fields = args
    if not fields:
        fields = list()
    data = document.to_mongodb()
    clean_output(data, *fields)
    return data



def clean_output(data, *fields):
    """
    Cleans a dict in-place, stripping out:
    * id
    * Notanormous' metadata
    * any blanks - Nones, empty strings, empty lists, empty dicts
    * any field called `metadata`
    * optionally, any fields you tell it to delete.
    
    Returns nothing.
    """
    if not fields:
        fields = list()
    for k in ___debone_toplevel___:
        data.pop(k, None)
    if '_data' in data:
        for k in ___debone_data___:
            data['_data'].pop(k, None)
    for f in fields:
        data.pop(f, None)
    for k in data.keys():
        # skip anything set to `False`:
        if isinstance(data[k], bool):
            continue
        # this will catch None or an empty string, list, or dict:
        if not data[k]:
            data.pop(k)
            continue
        if isinstance(data[k], dict):
            clean_output(data[k], fields)
            continue
        if isinstance(data[k], (list, tuple)):
            new_list = list()
            for i, item in enumerate(data[k]):
                if isinstance(item, dict):
                    # duh, remember, dicts get changed in-place:
                    clean_output(item, fields)
                    new_list.append(item)
                    continue
                new_list.append(item)
            data[k] = new_list


