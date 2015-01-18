# -*- coding: utf-8 -*-

"""
Notanormous
"""

from pymongo import ASCENDING, DESCENDING
from bson.objectid import ObjectId
from bson.dbref import DBRef
from fields import *
from document import *
from document import _make_document as make_document, _make_documents as make_documents
from util import make_embeddable, clean_output, cached_property
from notanormous.exceptions import ValidationError, FieldTypeError

