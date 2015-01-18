# -*- coding: utf-8 -*-

from __future__ import print_function

from collections import OrderedDict
import datetime
from pprint import pformat

from bson.objectid import ObjectId
from bson.dbref import DBRef
import pymongo
from pymongo.cursor import Cursor
from pymongo.errors import OperationFailure

from notanormous.fields import Field, EmbeddedDocumentField, ObjectIdField, \
    DBRefField, ListField, OrderedDictField
from notanormous.exceptions import ValidationError
from notanormous.util import make_embeddable


__all__ = [
    'Document',
    'DocumentMapSingleton',
    'EmbedOnlyAbuse',
    'ILLEGAL_FIELD_NAMES',
    'NoConnectionError',
    '_make_document',
    '_make_documents',
]

ILLEGAL_FIELD_NAMES = [
    'collection',
    'fields_to_load',
    'find',
    'get_by_id',
    'is_valid',
    'new_from_mongodb',
    'pre_save',
    'save',
    'save_prep',
    'to_mongodb',
    '_collection',
    '_container',
    '_data',
    '_dirty',
    '_fields',
    '_make_document',
    '_make_documents',
    '_set_db',
    '__index__',
    '__stored_properties__',
]

OPEN_DOCUMENTS = dict()
DOCUMENTS = []
COLLECTION_MAP = dict()  # mapping of collection name to class
DOCUMENT_MAP = {}  # mapping of Document classnames to their respective class
INDEXES_MADE = []  # list of indexes created so we don't repeat ourselves "classname.spec" where
# spec is a str of what came in on __index__


def update_open_documents(doc):
    if doc.__class__.__embed_only__:
        return doc
    clsname = doc.__class__.__name__
    if not doc._id:
        return doc
    OPEN_DOCUMENTS[clsname][doc._id] = doc
    return doc


def clear_open_documents(clsnames=None, mapping=None):
    if not clsnames:
        clsnames = DOCUMENT_MAP.keys()
    for clsname in clsnames:
        if DOCUMENT_MAP[clsname].__embed_only__:
            continue
        for _id, doc in OPEN_DOCUMENTS[clsname].iteritems():
            if not mapping:
                del doc
            if mapping and clsname in mapping and _id in mapping[clsname]:
                del doc
        if not mapping:
            OPEN_DOCUMENTS[clsname] = dict()


def get_open_document(clsname, _id):
    if clsname not in OPEN_DOCUMENTS:
        return None
    if _id in OPEN_DOCUMENTS[clsname]:
        return OPEN_DOCUMENTS[clsname][_id]
    return None


def _make_documents(result):
    """
    Take a result from pymongo and convert it back to a list of Document objects.
    
    Performance note: this will access everything in a search result, essentially using up
    any benefit from what would otherwise be lazy loading.
    
    :param result: may be a single result, cursor, or list.
    """
    global DOCUMENT_MAP
    is_cursor = isinstance(result, Cursor)
    if is_cursor:
        result = list(result)
    if not isinstance(result, list):
        result = [result]
    docs = []
    for item in result:
        if not item:
            continue
        doc = _make_document(item)
        if not doc:
            continue
        docs.append(doc)
    return docs


def _make_document(item):
    global DOCUMENT_MAP
    if not item:
        return None
    try:
        clsname = item['_data']['_classname']
    except Exception, msg:
        raise
    try:
        cls = DOCUMENT_MAP[item['_data']['_classname']]
    except KeyError:
        raise ValueError("You tried to make_documents from an improperly saved or otherwise unusable result. "
                         "(It did not contain a _classname I recognize.)")
    if item['_id'] in OPEN_DOCUMENTS[item['_data']['_classname']]:
        return OPEN_DOCUMENTS[item['_data']['_classname']][item['_id']]
    doc = cls.new_from_mongodb(item)
    doc = update_open_documents(doc)
    return doc


class NoConnectionError(Exception): pass


class EmbedOnlyAbuse(Exception): pass


class DocumentMapSingleton(object):
    def __new__(cls, *args, **kw):
        if not '_the_instance' in cls.__dict__:
            cls._the_instance = object.__new__(cls)
            cls.map = {}
            cls.indexes_made = []
        return cls._the_instance


def id_getter(field_spec, target_class):
    def wrap(target_class, field_spec):
        tc = target_class
        fs = field_spec

        def getter(self):
            if isinstance(tc, basestring):
                tc = fs.document.__document_map__[tc]
            try:
                r = make_documents(self._db[tc.__name__].find_one({"_id": getattr(self, fs.name)}))
            except Exception, msg:
                raise Exception("Could not load ID reference to a {0} from {1}: {2}".format(
                    tc, msg))
            return r[0]

        return getter

    return wrap(field_spec, target_class)


def get_reference_by_id(target_class, id_):
    try:
        r = _make_documents(target_class._collection().find_one({'_id': id_}))
    except Exception, msg:
        return None
    return r[0]


class DocumentMeta(type):
    def __init__(cls, name, bases, ns):
        # copy fields to class:
        cls._fields = {'_id': ObjectIdField()}
        if name != 'Document':
            DOCUMENTS.append(cls)
            DOCUMENT_MAP[name] = cls
            if not cls.__embed_only__:
                OPEN_DOCUMENTS[name] = dict()
            cls.__document_map__ = DocumentMapSingleton()
            cls.__document_map__.map[name] = cls
            # set collection
            if '__collection__' not in ns or not ns.get('__collection__', None):
                cls.__collection__ = name.lower()
            COLLECTION_MAP[cls.__collection__] = cls
        fields = dict()
        properties = []
        for field_name, field_spec in ns.iteritems():
            if isinstance(field_spec, Field) or \
                    (hasattr(field_spec.__class__, '__bases__') and Field in field_spec.__class__.__bases__):
                if callable(field_spec):
                    field_spec = field_spec()
                setattr(field_spec, 'name', field_name)
                setattr(field_spec, 'owner_document_classname', name)
                if field_name.startswith('_'):
                    raise ValueError("You cannot have a field name start with _.")
                if field_name in ILLEGAL_FIELD_NAMES:
                    raise ValueError("You cannot have a field named {0}".format(field_name))
                cls._fields[field_name] = field_spec
            # create automatic ObjectId and DBRef lookup properties:
            if isinstance(field_spec, ObjectIdField) and field_spec.document_class:
                ending = None
                if field_name.endswith('_id'):
                    ending = '_id'
                if field_name.endswith('_ref'):
                    ending = '_ref'
                if ending:
                    prop_name = field_name.split(ending)[0]

                    def setter_factory(aname):
                        def setter(self, obj):
                            return self._set_id_ref(aname, obj)

                        return setter

                    def getter_factory(aname):
                        def getter(self):
                            return self._get_id_ref(aname)

                        return getter

                    properties.append((prop_name, property(getter_factory(field_name),
                                                           setter_factory(field_name))))
            if isinstance(field_spec, ListField) and (
                        hasattr(field_spec, 'field') and isinstance(field_spec.field, (ObjectIdField, DBRefField))):
                ending = None
                generic = False
                if field_name.endswith('_ids'):
                    ending = '_ids'
                if field_name.endswith('_refs'):
                    ending = '_refs'
                if field_name.endswith('_references'):
                    ending = '_references'
                if not ending:
                    generic = True
                if ending:
                    prop_name = field_name.split(ending)[0]
                else:
                    prop_name = field_name + '_items'

                def setter_factory(aname):
                    def setter(self, items):
                        return self._set_id_refs(aname)

                    return setter

                def getter_factory(aname):
                    def getter(document):
                        return cls._get_id_refs(document, aname)

                    return getter

                properties.append((prop_name, property(getter_factory(field_name), setter_factory(field_name)) ))
        # now attach those properties:
        for prop_name, prop in properties:
            setattr(cls, prop_name, prop)


class Document(object):
    """
    Define your Document with commonly used fields where you want to validate, require, and 
    guarantee presence. These fields are accessed with dot syntax.
    
    Access with `dict` syntax to use arbitrary data fields. It's up to you to make
    sure the data in the `dict` is suitable for pymongo -> MongoDB.
    
    You can add any arbitrary data you want to the `dict` at `document._data` and it
    will be saved right along with the rest.
    
    Override `pre_output` method if you want to do some work on the data before it's converted to a `dict`
    and sent to pymongo.
    
    :param __stored_properties__: list of names of properties you've defined whose results should be
        stored. The data will be restored to the dictionary, so as not to clobber the property.
        This is only worthwhile for expensive to produce properties, or ones you'd like to index or
        access directly from pymongo for searching, etc.
    
    :param __index__: list of fields to index. Use a list to make a multi-field index.
    :param __unique__: list of fields index with `unique=True`
    :param __embed_only__: indicates a document which should only be embedded and never have a
        collection created for it. You can use __index__ and __unique__ with an
        embedded document.
    :param __auto_create__: Automatically create an instance of the embedded document with its defaults set
        if one does not exist at save time. Default is `False`. May not be `True` unless `__embed_only__` is
        also `True`. Has no effect if the `EmbeddedDocumentField` is inside a `ListField`.
    """

    __metaclass__ = DocumentMeta
    __stored_properties__ = []
    __index__ = []
    __unique__ = []
    __index_desc__ = []
    __embed_only__ = False
    __auto_create__ = False
    __version__ = 1
    __serial_index__ = False
    _db = None
    _indexes_created = False
    _collection = None
    collection = None
    _container = None
    _cache = None
    _data = {}

    def __init__(self, adict=None, **kw):
        if not adict:
            adict = dict()
        self._data = adict
        # save classname to help instantiate raw data
        self._data['_classname'] = self.__class__.__name__
        self._data['_version'] = self.__version__
        self._dirty = False
        self._id = None
        self._cache = {}
        if self.__auto_create__ is True and self.__embed_only__ is False:
            raise ValueError("You can only use __auto_create__ if __embed_only__ is True.")
        cls = self.__class__
        properties = []
        doc_map = DocumentMapSingleton()
        # set defaults from definitions - from the CLASS
        for field_name, field_obj in cls._fields.iteritems():
            instance_field_obj = self._fields[field_name]
            # set the document on this instance:
            instance_field_obj.name = field_name
            instance_field_obj.document = self
            # self._fields[field_name].document = self
            if isinstance(field_obj, EmbeddedDocumentField):
                target_document_class = field_obj.document_class
                if isinstance(target_document_class, basestring):
                    target_document_class = doc_map.map[target_document_class]
                if target_document_class.__auto_create__:
                    value = target_document_class()
                else:
                    value = None
            elif isinstance(field_obj, OrderedDictField):
                value = OrderedDict()
            elif isinstance(field_obj, ListField):
                if isinstance(field_obj.field, Field):
                    instance_field_obj.field.document = self
                    # WRONG: field_obj.field.document = self
                    # that was POSSIBLY? the cause of semi-merged objects.
                value = []
            else:
                default = getattr(field_obj, 'default', None)
                if callable(default):
                    value = default(self)
                else:
                    value = default
            super(Document, self).__setattr__(field_name, value)
        # set field values for existing fields, otherwise add to the dict:
        for key, value in kw.iteritems():
            if key in self._fields:
                setattr(self, key, value)
            else:
                self._data[key] = value
        if self._db:
            if not cls.__embed_only__:
                cls.create_indexes()
            if not cls._db:
                cls._set_db(self._db)


    def _get_id_ref(self, field_name):
        field = self._fields[field_name]
        tc = field.get_target_class()
        id_ = getattr(self, field_name)
        item = get_open_document(tc.__class__.__name__, id_)
        if item:
            return item
        item = tc._collection().find_one({'_id': id_})
        if not item:
            return None
        return tc.new_from_mongodb(item)


    def _set_id_ref(self, field_name, obj):
        field = self._fields[field_name]
        val = None
        if isinstance(obj, (ObjectId, DBRef)):
            val = obj
        if isinstance(obj, Document):
            if field_name.endswith('_ref') or field_name.endswith('reference'):
                val = obj.dbref
            else:
                val = obj._id
        if not val:
            raise ValueError("Attempt to assign invalid value to an ObjectIdField.")
        setattr(self, field_name, val)


    def _get_id_refs(self, field_name):
        refs = getattr(self, field_name)
        while None in refs:
            refs.remove(None)
        if len(refs) == 0:
            return []
        field_class = self._fields[field_name].field.get_target_class()
        if isinstance(refs[0], (ObjectIdField, int, long)):
            refs_to_find = list(refs)  # make a copy
            found = list()
            found_refs = list()
            for ref in refs_to_find:
                item = get_open_document(field_class, ref)
                if item:
                    found.append(item)
                    found_refs.append(ref)
            for ref in found_refs:
                refs_to_find.remove(ref)
            found.extend(_make_documents(list(field_class._collection().find({'_id': {'$in': refs_to_find}}))))
            sorted_items = sort_dicts_by_id_list(found, refs)
            return sorted_items
        # @TODO: fixup this to use OPEN_DOCUMENTS too.
        elif isinstance(refs[0], DBRef):
            refs_to_find = list(refs)
            found = list()
            found_refs = list()
            for dbref in refs:
                coll = dbref.collection
                _id = dbref.id
                clsname = COLLECTION_MAP[coll]
                item = get_open_document(clsname, _id)
                if item:
                    found.append(item)
                    found_refs.append(dbref)
            for found_ref in found_refs:
                refs_to_find.remove(found_ref)
            for dbref in refs_to_find:
                item = self._db.dereference(dbref)
                if not item:
                    continue
                found_refs.append(_make_document(item))
            # @TODO: fix this so it returns them in proper order
            return found_refs
        else:
            raise Exception(
                "Obviously we are barking up the wrong tree here. The first item in {0} is a {1}, I am a {2}".format(
                    field_name, refs[0].__class__.__name__, self.__class__.__name__))


    def _set_id_refs(self, field_name, items):
        """Sets all refs. Otherwise use append to the original field/attribute."""


    @classmethod
    def _collection(cls):
        if cls.__embed_only__:
            raise EmbedOnlyAbuse("Class {0} is embed-only, it has no collection.".format(cls.__name__))
        if not cls._db:
            raise Exception("cannot have the _collection without a _db")
        return getattr(cls._db, cls.__collection__)


    @staticmethod
    def _make_indexes(coll, prefix, index_list, fields, level=1):
        if level > 20:
            raise Exception("Over 20 levels deep? You need to re-think your document structure, weirdo.")
        if prefix:
            prefix += '.'
        for idx in index_list:
            if isinstance(idx, basestring):
                coll.create_index(prefix + idx)
            elif isinstance(idx, (list, tuple)):
                if isinstance(idx[0], (list, tuple)):
                    rewritten_specs = []
                    for multi_index_item in idx:
                        rewritten_specs.append((prefix + multi_index_item[0], multi_index_item[1]))
                    coll.create_index(rewritten_specs)
                else:
                    try:
                        new_spec = [(prefix + idx[0], idx[1])]
                        coll.create_index(new_spec)
                    except Exception, msg:
                        raise
            else:
                raise ValueError("Bad index spec: {0}".format(repr(idx)))
        for field_name, field in fields.iteritems():
            if isinstance(field, EmbeddedDocumentField):
                target_class = field.get_target_class()
                if isinstance(target_class, basestring):
                    target_class = DOCUMENT_MAP.get(target_class, None)
                Document._make_indexes(coll, prefix + field_name, target_class.__index__,
                                       target_class._fields, level + 1)
                continue
            if isinstance(field, ListField) and isinstance(field.field, EmbeddedDocumentField):
                target_class = field.field.document_class
                if isinstance(target_class, basestring):
                    target_class = DOCUMENT_MAP.get(target_class, None)
                if not target_class:
                    raise Exception("{0}'s Embedded document class does not exist!? WTF?")
                Document._make_indexes(coll, prefix + field_name, target_class.__index__,
                                       target_class._fields, level + 1)


    @classmethod
    def _drop_indexes(cls):
        if cls.__embed_only__:
            return
        coll = cls._collection()
        indexes = coll.index_information()
        for name in indexes.iterkeys():
            if name == '_id_':
                continue
            coll.drop_index(name)
        cls._indexes_created = False

    @classmethod
    def create_indexes(cls):
        if cls._indexes_created:
            return
        if cls.__embed_only__:
            return
        coll = cls._collection()
        Document._make_indexes(coll, '', cls.__index__, cls._fields)
        cls._indexes_created = True

    @classmethod
    def rebuild_indexes(cls):
        """
        You should run this on your class any time you change the `__index__` attribute in your class definition.
        """
        if cls.__embed_only__:
            return
        cls._drop_indexes()
        cls.create_indexes()


    # @TODO: This never gets called?!
    @classmethod
    def _set_db(cls, db):
        print("_set_db HAS BEEN CALLED!")
        raise Exception("Whoa. Stop!")
        global INDEXES_MADE
        cls._db = db
        if not cls.__embed_only__:
            cls.collection = cls._db.__collection__
        for sub in Document.__subclasses__():
            if sub.__embed_only__:
                continue
            # create indexes
            for field in getattr(sub, '__index__', []):
                unique = False
                if field in getattr(sub, '__unique__', []):
                    unique = True
                spec = cls.__name__ + '.' + str(field)
                if spec not in INDEXES_MADE:
                    try:
                        sub._collection().create_index(field, unique=unique)
                    except Exception, msg:
                        raise TypeError("Problem with indexing for class {0} regarding {1}. pymongo says: {2}" \
                                        .format(cls.__name__, field, msg))
                    INDEXES_MADE.append(spec)
            docfields = [f for f in sub._fields if isinstance(sub._fields[f], EmbeddedDocumentField)]
            for docfield_name in docfields:
                field = sub._fields[docfield_name]
                document_class = sub._fields[docfield_name].document_class
                if isinstance(document_class, basestring):
                    document_class = DocumentMapSingleton().map[document_class]
                for field in document_class.__index__:
                    ref = docfield_name + '.' + docfield_name
                    if ref not in INDEXES_MADE:
                        sub._collection().ensure_index(ref)
                        INDEXES_MADE.append(ref)
                for field in document_class.__unique__:
                    ref = docfield_name + '.' + docfield_name
                    if ref not in INDEXES_MADE:
                        sub._collection().ensure_index(ref, unique=True)
                        INDEXES_MADE.append(ref)


    @classmethod
    def find(cls, *pargs, **kargs):
        """
        Convenience method, just returns pymongo's `find` for the collection, which
        will return a pymongo Cursor object.
        
        You must still run the results through `make_documents` if you want full-featured Documents.
        Otherwise, you get the same as using pymongo directly.
        """
        return cls._collection().find(*pargs, **kargs)


    # @classmethod
    # def find(cls, query, sort=None, fields=None, raw=False):

    @classmethod
    def fields_to_load(cls):
        """
        Returns a list of field names suitable to load for most cases, leaving out stored properties.
        
        To be specific, it excludes all stored properties, and then includes `_data`.
        """
        fields = cls._fields.keys()
        fields.extend(['_data'])
        return fields


    @classmethod
    def get_by_id(cls, some_id, raw=False):
        if not isinstance(some_id, (int, long)):
            some_id = int(some_id)
        if some_id in OPEN_DOCUMENTS[cls.__name__]:
            return OPEN_DOCUMENTS[cls.__name__][some_id]
        coll = cls._collection()
        fields = cls.fields_to_load()
        item = coll.find_one({'_id': some_id}, fields=fields)
        if raw:
            return item
        return _make_document(item)


    def __getattribute__(self, key):
        oget = object.__getattribute__
        # if it's not a Field, just return the value, or bust
        if key == '_data':
            return oget(self, key)
        # support cached_property decorator
        if not key in oget(self, '_fields'):
            if key in oget(self, '_data'):
                return oget(self, '_data')[key]
            return oget(self, key)
        # use getter if necessary:
        if hasattr(oget(self, '_fields')[key], 'getter'):
            value = oget(self, key)
            try:
                return oget(self, '_fields')[key].getter(value, self.__class__._db)
            except TypeError:
                return None
        return oget(self, key)


    def __setattr__(self, key, value):
        if key in self._fields or hasattr(self, key):
            object.__setattr__(self, '_dirty', True)
            object.__setattr__(self, key, value)
        else:
            self._data[key] = value


    def __setitem__(self, key, value):
        object.__setattr__(self, '_dirty', True)
        if key in self._fields or hasattr(self, key):
            object.__setattr__(self, key, value)
        else:
            self._data[key] = value

    # implement self[key] style access
    def __getitem__(self, key):
        return self.get(key)


    def __len__(self):
        return len(self._data)

    def get(self, key, *args):
        oget = object.__getattribute__
        if key == '_data':
            return oget(self, '_data')
        default = NotGiven()
        if len(args) > 0:
            default = args[0]
        if key in self._fields or hasattr(self, key):
            if not isinstance(default, NotGiven):
                return getattr(self, key, default)
            return getattr(self, key)
        if key in self._data:
            if not isinstance(default, NotGiven):
                return self._data.get(key, default)
            return self._data.get(key)
        if not isinstance(default, NotGiven):
            return default
        raise KeyError(key)

    def items(self):
        return self._data.items()

    def iteritems(self):
        return self._data.iteritems()

    def keys(self):
        mykeys = self._data.keys()
        mykeys.extend(self._fields.keys())
        return mykeys

    def values(self):
        myvalues = self._data.values()
        myvalues.extend([object.__getattribute__(self, k) for k in self._fields.keys()])
        return myvalues

    def update(self, dict2):
        return self._data.update(dict2)

    def setdefault(self, key, default=None):
        return self._data.setdefault(key, default)

    def pop(self, key, default=None):
        return self._data.pop(key, default)

    def __delitem__(self, key):
        del self._data[key]


    def __contains__(self, key):
        return key in self._data or hasattr(self, key)


    @property
    def id(self):
        return self._id

    def is_valid(self):
        for field_name, field_def in self._fields.iteritems():
            field_value = getattr(self, field_name, None)
            if isinstance(field_def, EmbeddedDocumentField):
                # if field_def.required is True:
                return field_def.is_valid(field_value)
            if not field_def.is_valid(field_value):
                raise ValueError("For field {0} of class {1}, value {2} is not valid.".format(
                    field_name, self.__class__.__name__,
                    repr(getattr(self, field_name, None))))
        return True


    def pre_output(self):
        """
        """
        pass

    def to_mongodb(self):
        """Output a single dict with all fields and arbitrary data merged."""
        self.pre_output()
        d = dict()
        for key, field in self._fields.iteritems():
            orig_value = getattr(self, key, None)
            value = field.to_mongodb(orig_value)
            if value != orig_value:
                # now we can infer that the field's converter is overridden and we are done with
                # this field.
                d[key] = value
                continue
            value = to_mongo_output(field, value, self)
            d[key] = value
            if getattr(field, '_redefault', False) and not getattr(self, '_manual_set', False):
                if callable(field.default):
                    d[key] = field.default()
                else:
                    d[key] = field.default
        x = self._data.copy()
        cleanup_dict(x)
        d['_data'] = x
        d.pop('id', None)
        # remove empty ID:
        if not d.get('_id', None):
            d.pop('_id', None)
        for prop in self.__class__.__stored_properties__:
            d[prop] = getattr(self, prop, None)
        return d


    def post_save(self):
        """
        Override this method for post-save actions.
        """
        pass


    def clear_cached_property(self, prop_name):
        try:
            self._cache.pop(prop_name, None)
        except Exception:
            pass


    @staticmethod
    def pre_save(data):
        pass

    def save(self, safe=True, skip_refresh_stored_properties=False):
        if self.__class__.__embed_only__ is True:
            raise EmbedOnlyAbuse("You cannot save a {0} because it is marked embed-only." \
                                 .format(self.__class__.__name__))
        if not self.__class__._db:
            raise NoConnectionError
        if not self.is_valid():
            raise ValueError("Some field has bad data.")
        collection = self._collection()
        output = self.to_mongodb()
        self.__class__.pre_save(output)
        if not self._id:
            found = False
            i = 1
            c = collection.find(fields=['_id']).sort('_id', pymongo.DESCENDING).limit(1)
            if c.count(True) == 1:
                i = c.next()['_id'] + 1
            output['_id'] = i
            try:
                self._id = collection.insert(output, safe=safe, check_keys=True)
            except OperationFailure, msg:
                print("Oops, could not save myself!")
                raise
        else:
            collection.update({"_id": self._id}, output, multi=False, safe=safe)
        if not skip_refresh_stored_properties:
            try:
                self.refresh_stored_properties()
            except Exception, msg:
                print("Could not refresh_stored_properties on a {0} because {1}".format(
                    self.__class__.__name__, msg))
                raise
        self.post_save()
        return self


    def pre_delete(self):
        pass

    def delete(self):
        if not self._id:
            raise Exception("I was never saved, you can't delete me!")
        self.pre_delete()
        self._collection().remove({'_id': self._id})

    @property
    def dbref(self):
        coll = self._collection()
        if not coll:
            raise Exception("No collection.")
        if not self._id:
            raise Exception("No ID.")
        return DBRef(coll.name, self._id)  # , database=self._db.name)

    make_documents = staticmethod(_make_documents)
    make_document = staticmethod(_make_document)


    def refresh_stored_properties(self, proplist='not_given', prefix=None, coll=None):
        """
        Re-creates the list of stored properties and updates them to the database.
        Updates all stored properties if proplist is not provided.
        
        Recurses through EmbeddedDocumentField fields but not ListField fields which contain EmbeddedDocumentField
        fields. You are on your own to manage that mess.
        """
        if not coll:
            coll = self._collection()
        set_values_dict = dict()
        Document._get_stored_props(self, proplist=proplist, set_values_dict=set_values_dict)
        coll.update({'_id': self._id}, {'$set': set_values_dict})
        update_open_documents(self)
        return


    @staticmethod
    def _get_stored_props(obj, prefix=None, set_values_dict=None, proplist='not_given', recursion_level=1):
        """
        `set_values_dict` should be provided and will be altered in-place.
        """
        if not isinstance(set_values_dict, dict):
            raise ValueError("Must provide a dict for set_values_dict argument.")
        if recursion_level > 10:
            raise Exception("We got too many levels deep in recursion. Limit is 10.")
        if not prefix:
            prefix = u''
        else:
            prefix += u'.'
        manual_proplist = True
        if proplist == 'not_given' or not proplist:
            proplist = obj.__stored_properties__
            manual_proplist = False
        for propname in proplist:
            if hasattr(obj, '_cache'):
                if propname in obj._cache:
                    del obj._cache[propname]
            value = object.__getattribute__(obj, propname)
            set_values_dict[prefix + propname] = value
        if not manual_proplist:
            for field_name, field_spec in obj.__class__._fields.iteritems():
                if isinstance(field_spec, EmbeddedDocumentField):
                    embedded_doc = object.__getattribute__(obj, field_name)
                    if embedded_doc:
                        Document._get_stored_props(embedded_doc, prefix=field_name, set_values_dict=set_values_dict,
                                                   recursion_level=recursion_level + 1)
        return


    def embeddable(self, *args):
        return make_embeddable(self, *args)


    @staticmethod
    def new_document_from_dict(data, context_info=None):
        if not '_data' in data or not '_classname' in data['_data']:
            raise ValueError(
                "Not a Notanormous Document in dict form: {0} context_info={1}".format(repr(data), context_info))
        cls = DOCUMENT_MAP[data['_data']['_classname']]
        return cls.new_from_mongodb(data)

    @classmethod
    def new_from_mongodb(cls, data):
        """
        Create a new Document instance from pymongo data (a `dict`).
        """
        clsname = cls.__name__
        if not cls.__embed_only__:
            if not "_id" in data:
                raise ValueError(u"data from mongo should include an _id. data:\n{d}\n".format(d=pformat(data)))
            try:
                if data['_id'] in OPEN_DOCUMENTS[clsname]:
                    return OPEN_DOCUMENTS[clsname][data['_id']]
            except KeyError, msg:
                raise
        doc = cls()
        doc._from_mongodb(data)
        doc._dirty = False
        return update_open_documents(doc)

    def _from_mongodb(self, d):
        for key, value in d.iteritems():
            if key in self.__stored_properties__:
                continue
            if key == '_data':
                self._data = value
                continue
            if key in self._fields:
                field_spec = self._fields[key]
                if isinstance(field_spec, EmbeddedDocumentField):
                    if not value:
                        continue
                    embedded_doc = Document.new_document_from_dict(value, context_info=u'{0}.{1}'.format(
                        self.__class__.__name__, key))
                    embedded_doc._container = self
                    setattr(self, key, embedded_doc)
                    continue
                value = field_spec.from_mongodb(value)
                if isinstance(value, list):
                    if len(value) > 0:
                        if hasattr(field_spec, 'field') and isinstance(field_spec.field, EmbeddedDocumentField):
                            new_list = list()
                            for item in value:
                                embedded_doc = Document.new_document_from_dict(item, context_info=u'{0}.{1}'.format(
                                    self.__class__.__name__, key))
                                embedded_doc._container = self
                                new_list.append(embedded_doc)
                            value = new_list
                setattr(self, key, value)
            else:
                # anything that is not defined as a field in the class definition goes under `_data`:
                self._data[key] = value
        if '_id' in d:
            self._id = d['_id']

    def refresh(self):
        if not self._id:
            raise ValueError("Cannot refresh an unsaved Document.")
        data = self.__class__._collection().find_one({'_id': self._id}, fields=self.__class__.fields_to_load())
        if not data:
            raise ValueError("This document must have been deleted out from under you.")
        self._from_mongodb(data)
        update_open_documents(self)


    def __unicode__(self):
        return pformat(self.to_mongodb())

    def __str__(self):
        return unicode(self).encode('ascii', 'replace')

    __repr__ = __str__


def to_mongo_output(field, value, document):
    """
    Processes one field and value for Document's to_mongodb method.
    """
    orig_value = value
    value = field.to_mongodb(value)
    # If it's a DBRefField and a Document...
    if isinstance(field, DBRefField) and isinstance(value, Document):
        # don't allow creating a DBRef to an embed-only Document
        if value.__class__.__embed_only__ is True:
            raise Exception("Cannot create a DBRef to class {0} which is marked embed-only.".format(
                value.__class__.__name__))
        return value.dbref
    if isinstance(field, EmbeddedDocumentField):
        # don't allow a non-Document in an EmbeddedDocumentField
        if not isinstance(value, Document) and value is not None:
            raise Exception("EmbeddedDocumentField `{}` can only accept a Document. You gave me a {}" \
                            .format(field.name, value.__class__.__name__))
        if not field.required and not document._dirty:
            try:
                return value.to_mongodb()
            except (ValidationError, AttributeError):
                return None
        if value is None:
            return None
        return value.to_mongodb()
    if isinstance(field, ListField):
        if not isinstance(value, list):
            raise ValueError("Value for field {} must be a list. You gave me a {}".format(field.name,
                                                                                          value.__class__.__name__))
        result = []
        for item in value:
            if field.field:
                if isinstance(field.field, EmbeddedDocumentField):
                    result.append(item.to_mongodb())
                    continue
            result.append(item)
        return result
    if isinstance(field, EmbeddedDocumentField) and field.document_class.__auto_create__ is True:
        return field.document_class()
    # no other conditions met, just return the raw value
    return value


class NotGiven(object): pass


def cleanup_dict(d):
    # @TODO: move to util.py
    """
    Clean up a `dict` recursively.
    Removes None, empty string, and converts dates to datetimes (because pymongo cannot store dates for some reason), and times to a tuple ('datetime.time', h, m, s).
    """
    delete = []
    replace = {}
    for key, value in d.iteritems():
        if value is None or value == u'':
            delete.append(key)
            continue
        if isinstance(value, datetime.time):
            replace[key] = ('datetime.time', value.hour, value.minute, value.second)
            continue
        if isinstance(value, datetime.date):
            replace[key] = ('datetime.date', value.year, value.month, value.day)
            continue
        if isinstance(value, basestring) and not isinstance(value, unicode):
            replace[key] = unicode(value)
        if isinstance(value, dict):
            x = d[key].copy()
            cleanup_dict(x)
            replace[key] = x
            continue
        if isinstance(value, Document):
            x = d[key].to_mongodb()
            replace[key] = x
            continue
    for k in delete:
        d.pop(k)
    d.update(replace)


def sort_dicts_by_id_list(dict_list, id_list):
    """
    Returns a list of dicts, in the order of id_list.
    id_list should be a list of ID numbers.
    all dicts in dict_list must have an "_id" key.
    Items in dict_list but not in id_list (if any) are ignored.
    """
    # make a copy so we don't hose the objects original list of refs:
    use_ids = list(id_list)
    sorted_list = list()
    while len(use_ids) > 0:
        id_ = use_ids.pop(0)
        for i, some_dict in enumerate(dict_list):
            if some_dict['_id'] == id_:
                sorted_list.append(dict_list.pop(i))
    return sorted_list


def dictfind(dict_list, attr, val, exact=True):
    for somedict in dict_list:
        if somedict[attr] == val:
            return somedict
        




