import os
from .tables import TinyFatTable
from tinydb import TinyDB
from tinydb.storages import MemoryStorage


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
    @classmethod
    def create(cls, *args, name=TinyDB.DEFAULT_TABLE, table=TinyFatTable):
        try:
            db_path = args[0]
        except IndexError:
            db_path = None

        new_db = db_path is None or os.path.exists(db_path) is False
        storage = MemoryStorage if db_path is None else TinyDB.DEFAULT_STORAGE

        db = cls(*args, storage=storage, default_table=name, table_class=table)

        if new_db:
            db.purge_tables()
            db.table(name)

        return db
