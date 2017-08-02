# -*- coding: utf-8 -*-
import copy
import os
from contextlib import contextmanager

from tinydb import TinyDB
from tinydb.database import Table
from tinydb.storages import MemoryStorage
from tinydb.utils import itervalues

MODELS_DIR = os.path.join(os.path.dirname(__file__), "dbs")
if not os.path.exists(MODELS_DIR):
    os.mkdir(MODELS_DIR)


########################################################################
@contextmanager
def mock_all(table, elements):
    """
    Mocks the TinyFatDB.all method to return the given elements.
    Used by TinyFatQueryset to limit searches performed on the queryset
    to the elements stored on the instances '_elements' attribute.

    :param table: an instance of TinyFatTable
    :param elements: an iterable of Element objects
    """
    table.all = lambda: elements
    yield


########################################################################
class TinyFatDB(TinyDB):
    """
    Manages handling of TinyFatTable classes when creating new tables
    in the database. Stores the default table class on the
    attribute 'default_table_class', and handles additional custom
    tables when creating new tables via the TinyFatDB.table method.
    """

    ####################################################################
    def __init__(self, *args, **kwargs):
        """

        :param args:
        :param kwargs:
        """
        self.default_table_name = kwargs.get("default_table", TinyDB.DEFAULT_TABLE)
        self.default_table_class = kwargs.pop("table_class", TinyFatTable)
        super(TinyFatDB, self).__init__(*args, **kwargs)

    ####################################################################
    def table(self, name=None, table=None, **options):
        """
        Get access to a specific table.

        Creates a new table, if it hasn't been created before, otherwise it
        returns the cached :class:`~tinydb.Table` object.

        :param name: The name of the table.
        :type name: str
        :param table: A subclass of TinyFatTable
        """
        name = name or self.default_table_name
        self.table_class = table or self.default_table_class
        table = super(TinyFatDB, self).table(name, **options)
        self.table_class = self.default_table_class
        return table


########################################################################
class TinyFatModel(dict):
    """
    Base/default model class.
    Holds a single entry from a TinyFatDB table and enables adding methods
    to the entry data/dictionary in a "fat models" style.
    Also adds the 'eid' as a key/value pair on the element.
    """
    eid = None

    ###################################################################
    def __init__(self, element, **kwargs):
        """
        Load data from database into the current model instance.
        :param data: Element object from TinyDB
        """
        super(TinyFatModel, self).__init__(element, **kwargs)
        self.eid = self["eid"] = element.eid


########################################################################
class TinyFatQueryset:
    """
    All TinyFatTable methods that can return more than one element return
    an instance of TinyFatQueryset.
    Provides methods for performing further searches on the contained
    data, and other convenience methods including the ability to "refresh"
    the contained data from the database.
    Can be subclassed to add additional custom methods.
    """

    ####################################################################
    def __init__(self, table, elements, **kwargs):
        self.table = table
        self.model = table.model
        self.cond = kwargs.get("cond")
        self._elements = tuple(elements)

    ####################################################################
    def __len__(self):
        return len(self._elements)

    ####################################################################
    def __eq__(self, other):
        return tuple(self._elements) == tuple(other)

    ####################################################################
    def __iter__(self):
        return self.elements

    ####################################################################
    def __getitem__(self, item):
        return tuple(self.elements)[item]

    ####################################################################
    @property
    def elements(self):
        for el in self._elements:
            yield self.model(el)

    ####################################################################
    @property
    def eids(self):
        return tuple(self.values_list("eid"))

    ####################################################################
    def refresh_from_db(self):
        eids = self.eids
        self._elements = self.table.get_by_eids(eids=eids)
        if self.cond:
            self.table.clear_cache()
            self._elements = self.search(self.cond)

    ####################################################################
    @property
    def eids(self):
        return tuple(self.values_list("eid"))

    ####################################################################
    def refresh_from_db(self):
        eids = self.eids
        self._elements = self.table.get_by_eids(eids=eids)
        if self.cond:
            self.table.clear_cache()
            self._elements = self.search(self.cond)

    ####################################################################
    def qty(self):
        return len(self)

    ####################################################################
    def first(self):
        return self[0]

    ####################################################################
    def search(self, cond):
        with mock_all(self.table, self.elements):
            return self.table.search(cond)

    ####################################################################
    def values(self, *fields):
        if fields:
            for el in self.elements:
                yield {f: el[f] for f in fields}
        else:
            for el in self.elements:
                yield copy.deepcopy(el)

    ####################################################################
    def values_list(self, *fields):
        assert fields, "Must provide field names when calling TinyFatQueryset.values_list."

        if len(fields) == 1:
            field = fields[0]
            for el in self.elements:
                yield el[field]
        else:
            for el in self.elements:
                yield tuple(el[f] for f in fields)


########################################################################
def match_all_elements(eid):
    return True


########################################################################
class TinyFatTable(Table):
    """
    Base/default table class. Inherit's from TinyDB's Table model and
    handles loading database entries into model classes.
    """
    model = TinyFatModel

    ###################################################################
    def __init__(self, storage, cache_size=10):
        super(TinyFatTable, self).__init__(storage, cache_size=cache_size)
        self.fields = set()

    ###################################################################
    def search(self, cond):
        elements = (el for el in super(TinyFatTable, self).search(cond))
        return TinyFatQueryset(self, elements, cond=cond)

    ###################################################################
    def get(self, cond=None, eid=None):
        element = super(TinyFatTable, self).get(cond, eid)
        if element:
            return self.model(element)

    ###################################################################
    def get_by_eids(self, eids):
        elements = (self.get(eid=eid) for eid in eids)
        return TinyFatQueryset(self, elements)

    ###################################################################
    def all(self):
        elements = super(TinyFatTable, self).all()
        return TinyFatQueryset(self, elements)

    ###################################################################
    def count(self, cond):
        """
        Count the elements matching a condition.

        :param cond: the condition use
        :type cond: Query
        """
        return len(super(TinyFatTable, self).search(cond))

    ###################################################################
    def index(self, *fields):
        """
        Returns an Index instance (from package TinyIndex) that acts as a
        generator for all entries that include the given args in its keys.

        :param fields: dictionary keys that should be included in all indexed entries.
        :return: Index instance
        """

        elements = (el for el in self if all(f in el for f in fields))
        return TinyFatQueryset(self, elements)

    ###################################################################
    def unindexed(self, *fields):
        elements = (el for el in self if not all(f in el for f in fields))
        return TinyFatQueryset(self, elements)

    ####################################################################
    def first(self):
        for entry in itervalues(super(TinyFatTable, self)._read()):
            return self.model(entry)


########################################################################
def create_db(*args, name=TinyDB.DEFAULT_TABLE, table=TinyFatTable):
    try:
        db_path = args[0]
    except IndexError:
        db_path = None

    new_db = db_path is None or os.path.exists(db_path) is False
    storage = MemoryStorage if db_path is None else TinyDB.DEFAULT_STORAGE

    db = TinyFatDB(*args, storage=storage, default_table=name, table_class=table)

    if new_db:
        db.purge_tables()
        db.table(name)

    return db
