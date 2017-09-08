from . import utils


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
    def refresh_from_db(self):
        """
        Re-fetches fresh instances of all database entries
        stored in self._elements
        """
        self._elements = self.table.get_by_eids(eids=self.eids)
        if self.cond:
            self.table.clear_cache()
            self._elements = self.search(self.cond)

    ####################################################################
    @property
    def eids(self):
        return self.values("eid")

    ####################################################################
    def qty(self):
        return len(self)

    ####################################################################
    def first(self):
        return self[0]

    ####################################################################
    def search(self, cond):
        """
        Performs TinyDB.search, limiting the search to the elements stored
        on the '_elements' attribute.
        :param cond: TinyDB.Query instance.
        :return: TinyFatQueryset instance of matching elements.
        """
        with utils.mock_all(self.table, self.elements):
            return self.table.search(cond)

    ####################################################################
    def data(self, *fields):
        """
        Produces a dictionary with eid/element key/value pairs
        for each element in the queryset, containing only the fields
        provided in the 'fields' argument.

        :param fields: iterable of table field names
        :return: dictionary
        """
        if fields:
            element_data = {}
            for el in self.elements:
                data = {f: el[f] for f in fields}
                element_data[el.eid] = data
            return element_data
        else:
            raise ValueError("Must provide one or more fields as argument to {}.values".format(self.__class__))

    ####################################################################
    def values(self, field):
        """
        Produces a tuple of values for the given field for
        each element in the queryset.

        :param field: Table field name
        :return: tuple of values for the given field.
        """
        return tuple(el[field] for el in self.elements)
