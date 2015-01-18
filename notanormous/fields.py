# -*- coding: utf-8 -*-

from collections import OrderedDict
import datetime
from urlparse import urlparse
from urllib2 import urlopen, URLError

from bson.dbref import DBRef
from bson.objectid import ObjectId

from notanormous.connection import connection
from notanormous.exceptions import ValidationError, FieldTypeError

__all__ = [
    'BooleanField',
    'DateField',
    'DateTimeField',
    'DBRefField',
    'DictField',
    'EmbeddedDocumentField',
    'FieldTypeError',
    'FloatField',
    'IntegerField',
    'ListField',
    'LocationField',
    'ObjectIdField',
    'OrderedDictField',
    'StringField',
    'TimeField',
    'TupleField',
    'ValidationError',
    'WebURLField',
]


def is_int_or_long(x):
    return isinstance(x, (int, long))

def simple_validator(ftype):
    def valfunc(value):
        if ftype == 'any' or ftype is None:
            return True
        return isinstance(value, ftype)
    return valfunc

class Field(object):
    document = None
    name = None
    def __init__(self, ftype=None, validator=None, required=False, default=None):
        self.ftype = ftype or 'any'
        self.validator = validator or simple_validator(ftype)
        self.required = required
        self.default = default
    
    def is_valid(self, value):
        if not value and not self.required:
            return True
        return self.validator(value)
    
    def to_mongodb(self, value):
        return value
    
    def from_mongodb(self, value):
        return value
    
    @property
    def value(self):
        if not self.document or not self.name:
            raise Exception("Not initialized, cannot access my value.")
        return getattr(self.document, self.name)
    
    def __repr__(self):
        return self.__class__.__name__
    __str__ = __repr__


class StringField(Field):
    def __init__(self, max_length=None, choices=None, *args, **kw):
        self.max_length = max_length
        self.choices = choices
        self.ftype=unicode
        kw.pop('ftype', None)
        if not 'default' in kw:
            kw['default'] = u''
        super(StringField, self).__init__(*args, **kw)
    
    def is_valid(self, value):
        if not isinstance(value, basestring):
            return False
        if self.max_length and len(value) > self.max_length:
            return False
        if value and self.choices:
            vals = list()
            if isinstance(self.choices, OrderedDict):
                vals = self.choices.keys()
            elif isinstance(self.choices, (tuple, list)):
                for choice in self.choices:
                    if isinstance(choice, (tuple, list)):
                        vals.append(choice[0])
                    else:
                        vals.append(choice)
            else:
                raise ValueError("`choices` must be a tuple, list, or OrderedDict. You gave me a {}. (Field {})".format(self.choices.__class__.__name__, self.name))
            if not value in vals:
                return False
        return True


class WebURLField(StringField):
    def __init__(self, verify=False, *args, **kw):
        self.verify = verify
        kw['max_length'] = 1024
        super(WebURLField, self).__init__(*args, **kw)
    def is_valid(self, value):
        if not self.required and (not value or value == ''):
            return True
        if not StringField.is_valid(self, value):
            return False
        # check basic validity:
        parsed = urlparse(value)
        if parsed.scheme not in ['http', 'https']:
            return False
        if '.' not in parsed.netloc:
            return False
        if self.verify:
            try:
                u = urlopen(value)
            except URLError:
                return False
        return True


class IntegerField(Field):
    def __init__(self, *args, **kw):
        super(IntegerField, self).__init__(ftype=int, validator=is_int_or_long, *args, **kw)


class FloatField(Field):
    def __init__(self, *args, **kw):
        super(FloatField, self).__init__(ftype=float, *args, **kw)


class BooleanField(Field):
    def __init__(self, *args, **kw):
        if not 'default' in kw:
            kw['default'] = None
        super(BooleanField, self).__init__(ftype=bool, *args, **kw)


def now(*args):
    return datetime.datetime.now()


def today(*args):
    return datetime.date.today()


class DateTimeField(Field):
    """
    Note: automatic updates happen when calling to_mongodb, not each time you
    change a value.
    """
    def __init__(self, auto_now=False, auto_now_add=False, *args, **kw):
        self.auto_now = auto_now
        self.auto_now_add = auto_now_add
        self.ftype = datetime.datetime
        if self.auto_now_add or self.auto_now:
            kw['default'] = now
        if self.auto_now:
            self._redefault = True
        super(DateTimeField, self).__init__(*args, **kw)


class DateField(Field):
    """
    Note: automatic updates happen when calling to_mongodb, not each time you
    change a value.
    """
    def __init__(self, auto_today=False, auto_today_add=False, *args, **kw):
        self.auto_today = auto_today
        self.auto_today_add = auto_today_add
        self.ftype = datetime.date
        if self.auto_today_add or self.auto_today:
            kw['default'] = today
        if self.auto_today:
            self._redefault = True
        super(DateField, self).__init__(*args, **kw)
    
    def to_mongodb(self, value):
        if value is None:
            return None
        if not isinstance(value, (datetime.datetime, datetime.date)):
            raise ValueError("I Cannot store type {0} as a datetime.datetime. Value: {1}".format(
                             value.__class__.__name__, value))
        if not getattr(value, 'second', False):
            value = datetime.datetime(value.year, value.month, value.day)
        return value
    
    
    def from_mongodb(self, value):
        if value is None:
            return None
        return datetime.date(value.year, value.month, value.day)


class TimeField(Field):
    """
    Time has to be stored as a datetime, so we use now but replace h, m, s.
    """
    def __init__(self, *args, **kw):
        self.ftype = datetime.time
        
    
    def to_mongodb(self, value):
        if value is None:
            return None
        n = now()
        d = datetime.datetime(n.year, n.month, n.day, value.hour, value.minute, value.second)
        return d
    
    def from_mongodb(self, value):
        if value is None:
            return None
        return datetime.time(value.hour, value.minute, value.second)
    
    


class DBRefField(Field):
    
    def __init__(self, document_class=None, *args, **kw):
        self.document_class = document_class
        super(DBRefField, self).__init__(*args, **kw)
    
    
    def is_valid(self, value):
        if value is None and not self.required:
            return True
        document_class = self.document_class
        if isinstance(document_class, basestring):
            document_class = DOCUMENT_MAP[document_class]
        if not isinstance(value, DBRef) and (document_class and not isinstance(value, document_class)):
            raise ValidationError("Expected an ObjectId or {0}, got a {1}".format(
                                  self.document_class.__name__, value.__class__.__name__))
        return True
    
    def get_target_class(self):
        if isinstance(self.document_class, basestring):
            return self.document.__document_map__.map[self.document_class]
        return self.document_class
    
    def get_reference(self, value=None):
        if not value:
            value = self.get_value()
        tc = self.get_target_class()
        return tc.get_by_id(value.id)
    
    def get_value(self):
        return getattr(self.document, self.name)


class ObjectIdField(Field):
    def __init__(self, document_class=None, *args, **kw):
        self.document_class = document_class
        super(ObjectIdField, self).__init__(*args, **kw)
    
    def is_valid(self, value):
        if (value is None or value=='') and (not self.required):
            return True
        if self.required and not value:
            raise ValidationError("This field ({0}) is required.".format(self.name))
        if isinstance(value, basestring):
            try:
                value = ObjectId(value)
            except Exception, msg:
                print "Cannot make {0} into an ObjectId: {1}".format(value, msg)
                raise
        from notanormous.document import DocumentMapSingleton
        document_class = self.document_class
        doc_map = DocumentMapSingleton()
        if isinstance(document_class, basestring):
            document_class = doc_map.map[document_class]
        if isinstance(value, (ObjectId, int, long)) or (document_class and isinstance(value, document_class)):
            return True
        raise ValidationError("{0}.{1} expects an ObjectId or {2}, got a {3}".format(
                              self.document.__class__.__name__, self.name, document_class.__name__, value.__class__.__name__))
    def get_target_class(self):
        from notanormous.document import DocumentMapSingleton
        doc_map = DocumentMapSingleton()
        if isinstance(self.document_class, basestring):
            return doc_map.map[self.document_class]
        return self.document_class
    
    def get_reference(self, value=None):
        if not value:
            value = self.get_value()
        tc = self.get_target_class()
        r = tc.get_by_id(value)
        return r
    
    def get_value(self):
        return getattr(self.document, self.name)
    


class EmbeddedDocumentField(Field):
    def __init__(self, document_class, *args, **kw):
        self.document_class = document_class
        self.ftype = document_class
        super(EmbeddedDocumentField, self).__init__(*args, **kw)
    
    def get_target_class(self):
        from notanormous.document import DocumentMapSingleton
        doc_map = DocumentMapSingleton()
        try:
            if self.document and isinstance(self.document_class, basestring):
                return doc_map.map[self.document_class]
        except AttributeError, msg:
            raise Exception("Cannot get_target_class for field named {0}: {1}".format(self.name, msg))
        return self.document_class
    
    
    def is_valid(self, value):
        if value is None and not self.required:
            return True
        from notanormous.document import Document
        if not isinstance(value, Document):
            raise ValidationError("Trying to set an EmbeddedDocumentField to something other than a Document.")
        return True


class ListField(Field, list):
    def __init__(self, field=None, sorted=False, *args, **kw):
        if callable(field):
            self.field = field()
        else:
            self.field = field
        default = kw.pop('default', [])
        if not default:
            default = []
        if not isinstance(default, list):
            raise ValueError("A ListField's default must be a list!")
        super(ListField, self).__init__(ftype=list, default=default, *args, **kw)
    
    def to_mongodb(self, value):
        if self.field and hasattr(self.field, 'to_mongodb'):
            new_list = list()
            for item in value:
                new_list.append(self.field.to_mongodb(item))
            return new_list
        return value
    
    def from_mongodb(self, value):
        if self.field and hasattr(self.field, 'from_mongodb'):
            new_list = list()
            for item in value:
                new_list.append(self.field.from_mongodb(item))
            return new_list
        return value
    
    def is_valid(self, value):
        if not isinstance(value, list):
            raise ValidationError("The value of a ListField must be of type list. This is a {}.".format(value.__class__.__name__))
        if not self.required:
            return True
        if isinstance(self.field, EmbeddedDocumentField):
            from notanormous.document import Document
            for item in value:
                if not isinstance(item, Document):
                    raise ValidationError("list item of type {0} not allowed, Document expected.".format(
                        item.__class__.__name__))
        try:
            if self.field.is_valid(value):
                return True
            return False
        except:
            raise
        return True
    

class TupleField(Field, tuple):
    def __init__(self, field=None, sorted=False, *args, **kw):
        self.field = field
        default = kw.pop('default', None)
        if not default:
            default = []
        super(TupleField, self).__init__(ftype=tuple, default=default, *args, **kw)
    def is_valid(self, value):
        if not isinstance(value, tuple):
            return False
        return True


class DictField(Field, dict):
    """
    Be careful what you put in here, if it's not something MongoDB accepts, kaboom!
    """
    def __init__(self, field=None, *args, **kw):
        self.field = field
        default = kw.pop('default', {})
        if not default:
            default = {}
        super(DictField, self).__init__(ftype=dict, default=default, *args, **kw)
    
    def is_valid(self, value):
        if not value:
            return True
        if not isinstance(value, dict) and not isinstance(value, self.default.__class__):
            return False
        return True


class LocationField(Field, dict):
    """
    Should be a simple dict with keys lat, lon.
    """
    def __init__(self, field=None, *args, **kw):
        self.field = field
        default = kw.pop('default', None)
        if not default:
            default = None
        super(LocationField, self).__init__(ftype=dict, default=default, *args, **kw)
    
    def isinstance(self, value):
        if value is not None and not isinstance(value, dict):
            return False
        if 'lat' in value and 'lon' in value:
            if isinstance(value['lat'], (int, float, long)) and isinstance(value['lon'], (int, float, long)):
                return True
        return False

class OrderedDictField(Field, OrderedDict):
    """
    Be careful what you put in here, if it's not something MongoDB accepts, kaboom!
    """
    def __init__(self, field=None, *args, **kw):
        self.field = field
        default = kw.pop('default', OrderedDict())
        if not isinstance(default, OrderedDict):
            raise ValueError("An OrderedDictField's default must be an OrderedDict.")
        OrderedDict.__init__(self)
        Field.__init__(self, ftype=OrderedDict, default=default, *args, **kw)
    
    def is_valid(self, value):
        if not isinstance(value, OrderedDict):
            return False
        return True


