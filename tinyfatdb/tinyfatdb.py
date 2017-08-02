# -*- coding: utf-8 -*-
import copy
import os

from tinydb import TinyDB
from tinydb.database import Table
from tinydb.storages import MemoryStorage
from tinydb.utils import itervalues
from tinyindex import Index

MODELS_DIR = os.path.join(os.path.dirname(__file__), "dbs")
if not os.path.exists(MODELS_DIR):
    os.mkdir(MODELS_DIR)


########################################################################
class TinyFatDB(TinyDB):

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
        :param cache_size: How many query results to cache.
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
    """
    eid = None

    ###################################################################
    def __init__(self, element, **kwargs):
        """
        Load data from database into the model associated with the
        current instance.
        :param data: Element object from TinyDB
        :return: instance of self.model loaded with the given data and it's eid.
        """
        super(TinyFatModel, self).__init__(element, **kwargs)
        self.eid = self["eid"] = element.eid


########################################################################
class TinyFatQueryset:

    ####################################################################
    def __init__(self, elements, model):
        self.model = model
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
    def qty(self):
        return len(self)

    ####################################################################
    def first(self):
        return self[0]

    ####################################################################
    def search(self, cond):
        elements = (el for el in self.elements if cond(el))
        return TinyFatQueryset(elements, self.model)

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
def match_all(self):
    return True


########################################################################
class TinyFatTable(Table):
    """
    Base/default table class. Inherit's from TinyDB's Table model and
    handles loading database entries into model classes.
    """
    model = TinyFatModel
    match_all = match_all

    ###################################################################
    def __init__(self, storage, cache_size=10):
        super(TinyFatTable, self).__init__(storage, cache_size=cache_size)
        self.fields = set()

    ###################################################################
    def search(self, query):
        for data in super(TinyFatTable, self).search(query):
            yield self._transform(data)

    ###################################################################
    def get(self, cond=None, eid=None):
        element = super(TinyFatTable, self).get(cond, eid)
        if element:
            return self.model(element)

    ###################################################################
    def all(self):
        elements = super(TinyFatTable, self).all()
        return TinyFatQueryset(elements, model=self.model)

    ###################################################################
    def count(self, cond):
        """
        Count the elements matching a condition.

        :param cond: the condition use
        :type cond: Query
        """
        return len(super(TinyFatTable, self).search(cond))

    ###################################################################
    def index(self, *args, save_unindexed=False):
        """
        Returns an Index instance (from package TinyIndex) that acts as a
        generator for all entries that include the given args in its keys.

        :param args: dictionary keys that should be included in all indexed entries.
        :param save_unindexed: if True, saves all entries that do not contain all
        of the given keys as a list, accessible via Index.unindexed. Helpful
        for checking and handling entries that are missing desired data.
        :return: Index instance
        """
        return Index(self, *args, save_unindexed=save_unindexed)

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
