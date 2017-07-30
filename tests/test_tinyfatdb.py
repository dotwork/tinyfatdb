#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `tinyfatdb` package."""


import unittest

from tinydb import Query

from tinyfatdb.tinyfatdb import create_db, TinyFatTable


########################################################################
class ABCModel(dict):
    """
    Example custom model for data fetched from a TinyFatDB instance.
    """

    ####################################################################
    def sum(self, *fields):
        return sum(self[f] for f in fields)


########################################################################
class ABCTable(TinyFatTable):
    """
    Example custom table
    """
    model = ABCModel


########################################################################
class BaseTestMixin(unittest.TestCase):

    ####################################################################
    def setUp(self):
        super(BaseTestMixin, self).setUp()
        model_data = dict(ABC=dict(model=ABCModel, table_class=ABCTable))
        self.abc = create_db("ABC", model_data)

    ####################################################################
    def _add_foo_table(self):

        class FooModel(dict):
            def longest(self):
                return max(len(self[f]) for f in self)

        class FooTable(TinyFatTable):
            pass

        model_data = dict(Foo=dict(model=FooModel, table_class=FooTable))
        self.abc.model_data.update(model_data)
        return self.abc.table("Foo")


########################################################################
class TinyDBABCTest(BaseTestMixin):
    """
    """

    ####################################################################
    def test_add_table(self):
        self._add_foo_table()
        self.assertEqual({'Foo', 'ABC'}, self.abc.tables())

    ####################################################################
    def test_purge_table(self):
        self._add_foo_table()
        self.abc.purge_table("Foo")
        self.assertEqual({"ABC"}, self.abc.tables())

    ####################################################################
    def test_purge_tables(self):
        self._add_foo_table()
        self.abc.purge_tables()
        self.assertEqual(set(), self.abc.tables())

    ####################################################################
    def test_insert(self):
        self.assertEqual(0, len(self.abc))
        self.abc.insert({"a": 1})
        self.assertEqual(1, len(self.abc))

    ####################################################################
    def test_insert_multiple(self):
        self.assertEqual(0, len(self.abc))
        self.abc.insert_multiple([{"a": 1}, {"a": 1}])
        self.assertEqual(2, len(self.abc))

    ####################################################################
    def test_all(self):
        entries = [{"a": 1}, {"a": 1}]
        self.abc.insert_multiple(entries)
        self.assertEqual(entries, list(self.abc.all()))

    ####################################################################
    def test_get_by_eid(self):
        entry = {"a": 1}
        eid = self.abc.insert(entry)
        self.assertEqual(entry, self.abc.get(eid=eid))

    ####################################################################
    def test_get_by_condition(self):
        entries = [{"a": 1}, {"a": 2}, {"a": 3}]
        self.abc.insert_multiple(entries)
        self.assertEqual({"a": 2}, self.abc.get(Query().a > 1))

    ####################################################################
    def test_count(self):
        self.abc.insert_multiple([{"a": 1}, {"a": 2}])
        self.assertEqual(1, self.abc.count(Query().a > 1))

    ####################################################################
    def test_contains_by_eid(self):
        eid = self.abc.insert({"a": 1})
        bad_eid = 72
        self.assertTrue(self.abc.contains(eids=[eid, bad_eid]))
        self.assertFalse(self.abc.contains(eids=[bad_eid]))

    ####################################################################
    def test_contains_by_condition(self):
        self.abc.insert_multiple([{"a": 1}, {"a": 2}])
        self.assertTrue(self.abc.contains(Query().a > 1))
        self.assertFalse(self.abc.contains(Query().a > 2))

    ####################################################################
    def test_remove_by_eid(self):
        eid = self.abc.insert({"a": 1})
        self.abc.remove(eids=[eid])
        self.assertEqual(0, len(self.abc))

    ####################################################################
    def test_remove_by_condition(self):
        self.abc.insert_multiple([{"a": 1}, {"a": 2}])
        self.abc.remove(Query().a > 1)
        entries = list(self.abc)
        self.assertEqual(1, len(entries))
        self.assertEqual({"a": 1}, entries[0])

    ####################################################################
    def test_update_by_eids(self):
        eid_1 = self.abc.insert({"a": 1})
        eid_2 = self.abc.insert({"a": 1})

        self.abc.update({"a": 2}, eids=[eid_2])

        entry_1 = self.abc.get(eid=eid_1)
        entry_2 = self.abc.get(eid=eid_2)
        self.assertEqual(1, entry_1["a"])
        self.assertEqual(2, entry_2["a"])

    ####################################################################
    def test_update_by_condition(self):
        self.abc.insert_multiple([{"a": 1}, {"a": -1}])

        def make_absolute(e):
            e["a"] = abs(e["a"])

        self.abc.update(make_absolute, cond=Query().a < 0)
        for entry in self.abc:
            self.assertEqual({"a": 1}, entry)

    ####################################################################
    def test_first(self):
        self.assertEqual(None, self.abc.first())
        self.abc.insert_multiple([{"a": 1}, {"a": -1}])
        self.assertEqual({"a": 1}, self.abc.first())

    ####################################################################
    def test_index(self):
        """
        Should return the 3 entries with key 'a'.
        """
        entries = [
            dict(a=1, b=2, c=3),
            dict(a=1, b=2, c=3),
            dict(a=1, b=2, c=3),
            dict(b=2, c=3),
        ]
        self.abc.insert_multiple(entries)

        index = self.abc.index('a')
        self.assertEqual(entries[:-1], list(index))
        self.assertEqual([], index.unindexed)

    ####################################################################
    def test_unindexed(self):
        """
        Should return the 3 entries with key 'a'.
        Index.unindexed should contain the entry with no key 'a'.
        """
        entries = [
            dict(a=1, b=2, c=3),
            dict(a=1, b=2, c=3),
            dict(a=1, b=2, c=3),
            dict(b=2, c=3),
        ]
        self.abc.insert_multiple(entries)

        index = self.abc.index('a')
        self.assertEqual(entries[:-1], list(index))
        self.assertEqual([], index.unindexed)

    ####################################################################
    def test_multiple_indexes(self):
        """
        Should return the 2 entries that have the keys 'a' and 'b'.
        Index.unindexed should contain a list with the entries do not
        contain both keys 'a' and 'b'.
        """
        entries = [
            dict(b=2, c=3),
            dict(a=1, c=3),
            dict(a=1, b=2, c=3),
            dict(a=1, b=2, c=3),
        ]
        self.abc.insert_multiple(entries)

        index = self.abc.index('a', 'b', save_unindexed=True)

        self.assertEqual(entries[2:], list(index))
        self.assertEqual(entries[:2], index.unindexed)

    ####################################################################
    def test_foo(self):
        foo = self._add_foo_table()
        foo.first()


########################################################################
class TestFatModels(BaseTestMixin):

    ####################################################################
    def setUp(self):
        super(TestFatModels, self).setUp()
        self.foo = self._add_foo_table()

    ####################################################################
    def test_default_table_via_get(self):
        eid = self.abc.insert({"a": 1, "b": 2, "c": 3})
        entry = self.abc.get(eid=eid)
        self.assertEqual(6, entry.sum("a", "b", "c"))

    ####################################################################
    def test_default_table_via_first(self):
        self.abc.insert({"a": 1, "b": 2, "c": 3})
        entry = self.abc.first()
        self.assertEqual(6, entry.sum("a", "b", "c"))

    ####################################################################
    def test_default_table_via_all(self):
        self.abc.insert({"a": 1, "b": 2, "c": 3})
        entry = list(self.abc.all())[0]
        self.assertEqual(6, entry.sum("a", "b", "c"))

    ####################################################################
    def test_default_table_via_search(self):
        self.abc.insert({"a": 1, "b": 2, "c": 3})
        entry = list(self.abc.search(Query().a == 1))[0]
        self.assertEqual(6, entry.sum("a", "b", "c"))

    ####################################################################
    def test_default_table_via_index(self):
        self.abc.insert({"a": 1, "b": 2, "c": 3})
        entry = list(self.abc.index("a"))[0]
        self.assertEqual(6, entry.sum("a", "b", "c"))

    ####################################################################
    def test_additional_table_via_get(self):
        eid = self.foo.insert({"short": "a", "medium": "aa", "long": "aaa"})
        entry = self.foo.get(eid=eid)
        self.assertEqual(3, entry.longest())

    ####################################################################
    def test_additional_table_via_first(self):
        self.foo.insert({"short": "a", "medium": "aa", "long": "aaa"})
        entry = self.foo.first()
        self.assertEqual(3, entry.longest())

    ####################################################################
    def test_additional_table_via_all(self):
        self.foo.insert({"short": "a", "medium": "aa", "long": "aaa"})
        entry = list(self.foo.all())[0]
        self.assertEqual(3, entry.longest())

    ####################################################################
    def test_additional_table_via_search(self):
        self.foo.insert({"short": "a", "medium": "aa", "long": "aaa"})
        entry = list(self.foo.search(Query().short == "a"))[0]
        self.assertEqual(3, entry.longest())

    ####################################################################
    def test_additional_table_via_index(self):
        self.foo.insert({"short": "a", "medium": "aa", "long": "aaa"})
        entry = list(self.foo.index("short"))[0]
        self.assertEqual(3, entry.longest())
