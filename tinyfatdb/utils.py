# -*- coding: utf-8 -*-
from contextlib import contextmanager


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
