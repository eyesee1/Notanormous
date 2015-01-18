# -*- coding: utf-8 -*-

import datetime
from pprint import pprint, pformat
from unittest import TestCase

from pymongo.database import DBRef
from notanormous.document import Document, DOCUMENTS, _make_document as make_document, \
                                 _make_documents as make_documents, \
                                 sort_dicts_by_id_list
from notanormous.fields import *
from notanormous.util import cached_property

from pymongo.connection import Connection
from pymongo.errors import ConnectionFailure

connection = Connection('localhost')
db = connection['notanormous_tests']
Document._db = db

# some Document classes used for testing
class SomeDoc(Document):
    title                 = StringField(required=True)
    content               = StringField()
    misc                  = StringField(default=u'elderberry')
    odict                 = OrderedDictField()
    xdates                = ListField(DateField())
    
    __stored_properties__ = ['stuff']
    __serial_index__      = True
    
    @property
    def stuff(self):
        return 2 + 2

class EmbedMe(Document):
    thing1 = StringField(required=True)
    thing2 = StringField(max_length=16)
    __embed_only__ = True

class Coord(Document):
    x = IntegerField()
    y = IntegerField()
    __serial_index__ = True

CHOICES = ((u'a', u'A'), (u'b', u'B'))

class Something(Document):
    name               = StringField()
    choicy             = StringField(choices=CHOICES)
    things             = EmbeddedDocumentField(EmbedMe)
    manythings         = ListField(EmbeddedDocumentField(EmbedMe))
    coords             = ListField(EmbeddedDocumentField(Coord))
    words              = ListField(StringField)
    created            = DateTimeField(auto_now_add=True)
    mod                = DateTimeField(auto_now=True)
    other_coord        = DBRefField()
    times_added_coords = 0
    
    __serial_index__   = True
    
    @cached_property()
    def add_coords(self):
        """
        Absurdly add up the coords, for the purpose of testing `cached_property`.
        """
        t = 0
        for co in self.coords:
            t += co.x + co.y
        self.times_added_coords = self.times_added_coords + 1
        return t


def droptestdb():
    connection.drop_database('notanormous_tests')

class TestDocuments(TestCase):
    def test_documents(self):
        droptestdb()
        doc = SomeDoc(title=u'spam')
        assert doc.__collection__ == 'somedoc'
        # test dict style access for arbitrary data
        doc['sausage'] = u'eggs'
        assert doc.is_valid() == True
        dump = doc.to_mongodb()
        assert dump['title'] == u'spam'
        assert dump['_data']['sausage'] == u'eggs'
        assert dump['misc'] == u'elderberry'
        assert dump['stuff'] == 4
        assert isinstance(dump['misc'], unicode)
        # test disallowing fields that start with underscore
        try:
            class BogusDoc(Document):
                _data = StringField()
                foo = StringField()
        except Exception, err:
            assert err.__class__.__name__ == 'ValueError'
        # test ordered dict field
        del doc
        doc = SomeDoc()
        doc.odict['foo'] = 'bar'
        doc.odict['barre'] = 'fooe'
        doc.save()
        p = doc.odict.popitem()
        assert p[0] == 'barre'
        print DOCUMENTS
        droptestdb()
        print "Done."
    
    def test_datastore(self):
        droptestdb()
        
        doc = SomeDoc(title=u'test')
        doc.fart = 'poo'
        doc.save()
        assert doc['fart'] == 'poo'
        assert doc._data['fart'] == 'poo'
        doc._data['unladen'] = 'swallow'
        doc.save()
        assert doc['unladen'] == 'swallow'
        print "Two methods of accessing _data worked:"
        pprint(doc.to_mongodb(), width=135, indent=4)
        droptestdb()
    
    
    def test_attr_dict_access(self):
        droptestdb()
        x = Something(name=u'Unladen Swallow').save()
        assert x.name == x['name']
        assert x['name'] == x.name
        x['not_a_field'] = 10
        assert x.not_a_field == 10
        droptestdb()
    
    
    def test_lists_dont_merge(self):
        """
        Attempt to check for the problem of data going to the wrong Document instance.
        """
        droptestdb()
        x = Something(name=u'Unladen Swallow').save()
        y = Something(name=u'Bunny').save()
        words1 = ['gnashing', 'teeth', 'grail']
        words2 = ['shrubbery', 'halibut']
        x.words = words1
        x.save()
        assert 'gnashing' not in y.words
        assert 'gnashing' in x.words
        y.words = words2
        y.save()
        assert 'gnashing' not in y.words
        assert 'gnashing' in x.words
        assert 'halibut' in y.words
        assert 'halibut' not in x.words
        del x, y
        e1 = EmbedMe(thing1='mattress')
        e2 = EmbedMe(thing1='dog kennels')
        x = Something(manythings=[e1]).save()
        y = Something(manythings=[e1, e2]).save()
        assert x.manythings[0] is e1
        assert len(x.manythings) == 1
        assert e2 not in x.manythings
        assert e1 in x.manythings
        assert e1 in y.manythings
        droptestdb()
    
    
    def test_embedded_documents(self):
        droptestdb()
        x = Something(name=u'Unladen Swallow')
        e = EmbedMe(thing1=u'sausage')
        x.things = e
        x.coords.append(Coord(x=2, y=4))
        x.coords.append(Coord(x=5, y=9))
        try:
            x.coords.append(42)
            print "I was allowed to append a non Document where only a Document should go."
        except ValueError:
            assert 1 == 1
        x.coords.remove(42)
        other_coord = Coord(x=2, y=6)
        other_coord.save()
        print "other_coord._id={0}".format(other_coord._id)
        x.other_coord = other_coord.dbref
        dump = x.to_mongodb()
        print "Test with embedded:"
        pprint(dump)
        
        assert isinstance(dump['other_coord'], DBRef)
        # test a fake load of embedded doc
        data = {
            '_id': 99999,
            'name': 'Sir Robin',
            '_classname': 'Something',
            'things': {
                'thing1': 'Run Away!',
                'thing2': 'Nih!',
                '_data': { '_id': 412312311, '_classname': 'EmbedMe' }
            },
            'coords': [
                {'x': 2, 'y': 4, '_id': 412312312, '_data': { '_classname': 'Coord' } },
                {'x': 5, 'y': 9, '_id': 412312313, '_data': { '_classname': 'Coord' } },
            ],
        }
        y = Something.new_from_mongodb(data)
        print "Test load of embedded:"
        pprint(y.to_mongodb())
        assert isinstance(y.things, EmbedMe)
        print "y.coords = ", y.coords
        assert isinstance(y.coords[0], Coord)
        assert y._dirty is False
        y.name = 'spam'
        assert y._dirty is True
        e.thing2 = u'aoeuhtnsaoeuhtnsaoeuhtnsaoeuhtns'
        try:
            e.is_valid()
            assert 0/0
        except ValueError, msg:
            assert True is True
        print DOCUMENTS
        droptestdb()
    
    
    def test_choice_validation(self):
        droptestdb()
        x = Something(name=u'Unladen Swallow')
        e = EmbedMe(thing1=u'sausage')
        choicetest = Something(name=u'hello', choicy=u'a', things=e)
        assert choicetest.is_valid() is True
        choicetest.choicy = u'c'
        try:
            valid = choicetest.is_valid()
            print "WARNING: Validation failed for something."
            print "This error means that choice list validation is not yet implemented:"
        except ValueError, msg:
            assert True
        x.things = e
        x.coords.append(Coord(x=2, y=4))
        x.coords.append(Coord(x=5, y=9))
        try:
            x.coords.append(42)
            print "I was allowed to append a non Document where only a Document should go."
        except ValueError:
            assert 1 == 1
    
    
    def test_cached_properties(self):
        return True
        # not implemented yet, so skip these tests for now:
        droptestdb()
        s = Something(name='foo', coords=[Coord(x=2, y=2), Coord(x=3, y=3)]).save()
        assert s.times_added_coords == 0
        print "Added coords:", s.add_coords
        assert s.times_added_coords == 1
        print "Added coords again:", s.add_coords
        assert s.times_added_coords == 1
        print "OK, we only added coords once! Noyce."
        droptestdb()
    
    
    def test_ordered_ref_returns(self):
        dict_list = [dict(_id=2, x=1), dict(_id=43, x=2), dict(_id=988, x=3), dict(_id=283, x=4), dict(_id=5, x=5)]
        id_list = [988, 43, 5, 2]
        print "Dicts should be in this order:", id_list
        print sort_dicts_by_id_list(dict_list, id_list)
    
    def test_listfield_of_dates(self):
        droptestdb()
        s = SomeDoc()
        today = datetime.date.today()
        s.xdates.append(today)
        some_other_date = datetime.date(2002, 11, 12)
        s.xdates.append(some_other_date)
        s.save()
        del s
        s = make_document(db.somedoc.find()[0])
        print "s.xdates is {0}".format(s.xdates)
        assert s.xdates[0] == today
        assert s.xdates[1] == some_other_date
        droptestdb()
    
