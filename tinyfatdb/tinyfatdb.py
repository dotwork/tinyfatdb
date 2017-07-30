# -*- coding: utf-8 -*-
import os

from tinydb import TinyDB
from tinydb.database import Table, StorageProxy
from tinydb.storages import MemoryStorage
from tinydb.utils import itervalues
from tinyindex import Index

MODELS_DIR = os.path.join(os.path.dirname(__file__), "dbs")
if not os.path.exists(MODELS_DIR):
    os.mkdir(MODELS_DIR)


########################################################################
class TinyFatDB(TinyDB):

    ####################################################################
    def __init__(self, model_data, *args, **kwargs):
        self.model_data = model_data
        super(TinyFatDB, self).__init__(*args, **kwargs)

    ####################################################################
    def table(self, name=TinyDB.DEFAULT_TABLE, **options):
        """
        Get access to a specific table.

        Creates a new table, if it hasn't been created before, otherwise it
        returns the cached :class:`~tinydb.Table` object.

        :param name: The name of the table.
        :type name: str
        :param cache_size: How many query results to cache.
        """

        if name in self._table_cache:
            return self._table_cache[name]

        table_class = self.model_data[name]["table_class"]
        model = self.model_data[name]["model"]
        table_class.model = model

        table = table_class(StorageProxy(self._storage, name), **options)

        self._table_cache[name] = table

        # table._read will create an empty table in the storage, if necessary
        table._read()

        return table


########################################################################
class TinyFatTable(Table):
    model = None

    ###################################################################
    def _transform(self, data):
        """
        Load data from database into the model associated with the
        current instance.
        :param data: Element object from TinyDB
        :return: instance of self.model loaded with the given data and it's eid.
        """
        if data is not None:
            # Create a model with the data
            model = self.model(data)

            # Store the model's eid
            model.eid = data.eid

            return model

    ###################################################################
    def search(self, query):
        for data in super(TinyFatTable, self).search(query):
            yield self._transform(data)

    ###################################################################
    def get(self, cond=None, eid=None):
        element = super(TinyFatTable, self).get(cond, eid)
        return self._transform(element)

    ###################################################################
    def all(self):
        return (self._transform(data) for data in super(TinyFatTable, self).all())

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
            return self._transform(entry)


########################################################################
def create_db(name, model_data, json_filepath=None, in_memory=True):
    if not json_filepath:
        filename = "{name}.json".format(name=name.lower())
        json_filepath = os.path.join(MODELS_DIR, filename)
    new_db = in_memory or not os.path.exists(json_filepath)

    if in_memory:
        db = TinyFatDB(storage=MemoryStorage, default_table=name, model_data=model_data)
    else:
        db = TinyFatDB(json_filepath, default_table=name, model_data=model_data)

    if new_db:
        db.purge_tables()
        db.table(name)

    return db
