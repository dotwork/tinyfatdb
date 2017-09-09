from .models import TinyFatModel
from .querysets import TinyFatQueryset
from tinydb.database import Table
from tinydb.utils import itervalues


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
        """
        Wraps TinyDB's Table.search method with a TinyFatQueryset.

        :param cond: TinyDB Query instance
        :return: TinyFatQueryset instance
        """
        elements = (el for el in super(TinyFatTable, self).search(cond))
        return TinyFatQueryset(self, elements, cond=cond)

    ###################################################################
    def get(self, cond=None, eid=None):
        """
        Wraps TinyDB's Table.get method to return an instance of the
        associated TinyFatModel class.

        :param cond: TinyDB Query instance
        :param eid: TinyDB Element eid
        :return: TinyFatModel instance
        """
        element = super(TinyFatTable, self).get(cond, eid)
        return self.model(element) if element else None

    ###################################################################
    def get_by_eids(self, eids):
        """
        Returns a TinyFatQueryset of elements matching the given eids.

        :param eids: iterable of Element eids
        :return: TinyFatQueryset
        """
        elements = (self.get(eid=eid) for eid in eids)
        return TinyFatQueryset(self, elements)

    ###################################################################
    def all(self):
        """
        Wrapper around TinyDB's Table.all method that returns a
        TinyFatQueryset of all elements contained in table.

        :return: TinyFatQueryset
        """
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
        Returns a TinyFatQueryset containing all elements in the table
        that include the given fields in its keys.

        :param fields: table fields that should be included in all indexed entries.
        :return: TinyFatQueryset instance
        """
        elements = (el for el in self if all(f in el for f in fields))
        return TinyFatQueryset(self, elements)

    ###################################################################
    def unindexed(self, *fields):
        """
        Returns a TinyFatQueryset containing all elements in the table
        that do _not_ include the given fields in its keys.

        :param fields: Table fields that should be excluded in all indexed entries.
        Will match element if any of the given fields are missing.

        :return: TinyFatQueryset instance
        """
        elements = (el for el in self if not all(f in el for f in fields))
        return TinyFatQueryset(self, elements)

    ####################################################################
    def first(self):
        """
        Convenience method to return the first item in the table.
        :return: TinyFatModel instance
        """
        for entry in itervalues(super(TinyFatTable, self)._read()):
            return self.model(entry)
