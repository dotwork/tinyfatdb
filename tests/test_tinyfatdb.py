#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import os
from tempfile import NamedTemporaryFile
from unittest import TestCase

from tinydb import Query as Q, TinyDB
from tinydb.storages import MemoryStorage

from tinyfatdb.tinyfatdb import create_db, TinyFatTable, TinyFatDB, match_all_elements


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
    Example custom table.
    """
    model = ABCModel


########################################################################
class FooModel(dict):
    """
    Additional model to test adding a second model to database.
    """

    ####################################################################
    def longest(self):
        return max(len(self[f]) for f in self)


########################################################################
class FooTable(TinyFatTable):
    """
    Additional table to test adding a second table to database.
    """
    model = FooModel


########################################################################
a_ends_with_2 = Q().a.test(lambda a: a.endswith("2"))


########################################################################
class BaseTableAndModelTest:
    _entries = ({"a": "A1", "b": "B1", "c": "C1"},
                {"a": "A2", "b": "B2", "c": "C2"},
                {"a": "A3", "b": "B3", "c": "C3"})

    ####################################################################
    def insert_entries(self, entries):
        eids = self.db.insert_multiple(entries)
        for entry in entries:
            entry.update({"eid": eids.pop(0)})
        return entries

    ####################################################################
    def test_purge_tables(self):
        self.db.table("Foo", table=FooTable)
        self.db.purge_tables()
        self.assertEqual(set(), self.db.tables())

    ####################################################################
    def test_insert(self):
        self.assertEqual(len(self.entries), len(self.db))
        self.db.insert({})
        self.assertEqual(len(self.entries) + 1, len(self.db))

    ####################################################################
    def test_insert_multiple(self):
        self.assertEqual(len(self.entries), len(self.db))
        self.db.insert_multiple(({}, {}))
        self.assertEqual(len(self.entries) + 2, len(self.db))

    ####################################################################
    def test_all(self):
        self.assertEqual(self.entries, tuple(self.db.all()))

    ####################################################################
    def test_get_by_eid(self):
        entry = self.entries[0]
        self.assertEqual(entry, self.db.get(eid=entry["eid"]))

    ####################################################################
    def test_get_by_condition(self):
        self.assertEqual(self.entries[1], self.db.get(a_ends_with_2))

    ####################################################################
    def test_count(self):
        self.assertEqual(1, self.db.count(Q().a == "A1"))
        self.assertEqual(3, self.db.count(Q().a.test(lambda val: "A" in val)))

    ####################################################################
    def test_contains_by_eid(self):
        good_eid = self.entries[0]["eid"]
        bad_eid = 72
        self.assertTrue(self.db.contains(eids=[bad_eid, good_eid]))
        self.assertFalse(self.db.contains(eids=[bad_eid]))

    ####################################################################
    def test_contains_by_condition(self):
        self.assertTrue(self.db.contains(a_ends_with_2))
        self.assertFalse(self.db.contains(Q().a == "foo"))

    ####################################################################
    def test_remove_by_eid(self):
        self.assertEqual(3, len(self.db))
        self.db.remove(eids=[self.entries[0]["eid"]])
        self.assertEqual(2, len(self.db))
        self.db.remove(eids=[self.entries[1]["eid"], self.entries[2]["eid"]])
        self.assertEqual(0, len(self.db))

    ####################################################################
    def test_remove_by_condition(self):
        self.assertEqual(3, len(self.db))
        self.db.remove(Q().a == "A1")
        self.assertEqual(2, len(self.db))
        self.db.remove(Q().a.test(lambda val: val in ("A2", "A3")))
        self.assertEqual(0, len(self.db))

    ####################################################################
    def test_update_by_eids(self):
        eid_1 = self.db.insert({"a": 1})
        eid_2 = self.db.insert({"a": 1})

        self.db.update({"a": 2}, eids=[eid_2])

        entry_1 = self.db.get(eid=eid_1)
        entry_2 = self.db.get(eid=eid_2)
        self.assertEqual(1, entry_1["a"])
        self.assertEqual(2, entry_2["a"])

    ####################################################################
    def test_update_by_condition(self):

        def lower(val):
            val["a"] = val["a"].lower()

        self.db.update(lower, cond=Q().a.test(lambda val: "A" in val))
        for entry in self.db:
            self.assertTrue("a" in entry["a"])

    ####################################################################
    def test_first(self):
        self.assertEqual(self.entries[0], self.db.first())
        self.db.remove(match_all_elements)
        self.assertEqual(None, self.db.first())

    ####################################################################
    def test_index(self):
        """
        Should return the 3 entries with key 'a'.
        """
        self.db.remove(match_all_elements)
        entries = (
            {"a": 1, "b": 2, "c": 3},
            {"a": 1, "b": 2, "c": 3},
            {"a": 1, "b": 2, "c": 3},
            {"b": 2, "c": 3},
        )
        self.insert_entries(entries)
        index = self.db.index('a')
        self.assertEqual(entries[:-1], tuple(index))

    ####################################################################
    def test_unindexed(self):
        """
        Should return a queryset with the entry with no key 'a'.
        """
        self.db.remove(match_all_elements)
        entries = (
            {"a": 1, "b": 2, "c": 3},
            {"a": 1, "b": 2, "c": 3},
            {"a": 1, "b": 2, "c": 3},
        )
        self.insert_entries(entries)
        eid = self.db.insert({"b": 2, "c": 3})
        element = self.db.get(eid=eid)

        queryset = self.db.unindexed('a')
        self.assertEqual((element, ), tuple(queryset))

    ####################################################################
    def test_multiple_indexes(self):
        """
        Should return the 2 entries that have the keys 'a' and 'b'.
        Index.unindexed should contain a list with the entries do not
        contain both keys 'a' and 'b'.
        """
        self.db.remove(match_all_elements)
        entries = (
            {"b": 2, "c": 3},
            {"a": 1, "c": 3},
            {"a": 1, "b": 2, "c": 3},
            {"a": 1, "b": 2, "c": 3},
        )
        self.insert_entries(entries)
        index = self.db.index('a', 'b')
        self.assertEqual(entries[2:], tuple(index))


########################################################################
class TestDefaultTableAndModel(TestCase, BaseTableAndModelTest):
    """
    Tests to ensure original TinyDB and TinyFatTable functions work
    with default TinyFatDB, TinyFatTable, and TinyFatModel classes.
    """

    ####################################################################
    def setUp(self):
        self.db = create_db()
        self.entries = self.insert_entries(self._entries)

    ####################################################################
    def test_add_table(self):
        self.db.table("Foo", table=FooTable)
        self.assertEqual({'Foo', TinyDB.DEFAULT_TABLE}, self.db.tables())

    ####################################################################
    def test_purge_table(self):
        self.db.table("Foo", table=FooTable)
        self.db.purge_table("Foo")
        self.assertEqual({TinyDB.DEFAULT_TABLE}, self.db.tables())


########################################################################
class TestCustomTableAndModel(TestCase, BaseTableAndModelTest):
    """
    Tests to ensure original TinyDB and TinyFatTable functions work
    with subclasses of TinyFatDB, TinyFatTable, and TinyFatModel classes.
    """

    ####################################################################
    def setUp(self):
        super(TestCustomTableAndModel, self).setUp()
        self.db = create_db(name="ABC", table=ABCTable)
        self.entries = self.insert_entries(self._entries)

    ####################################################################
    def test_add_table(self):
        self.db.table("Foo", table=FooTable)
        self.assertEqual({"Foo", "ABC"}, self.db.tables())

    ####################################################################
    def test_purge_table(self):
        self.db.table("Foo", table=FooTable)
        self.db.purge_table("Foo")
        self.assertEqual({"ABC"}, self.db.tables())


########################################################################
class TestFatModels(TestCase):
    """
    Tests to ensure custom methods on instances of TinyFatModel work,
    both for the default table and for additional tables.
    """

    ####################################################################
    def setUp(self):
        super(TestFatModels, self).setUp()
        self.abc = create_db(name="ABC", table=ABCTable)
        self.foo = self.abc.table(name="Foo", table=FooTable)

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
        entry = list(self.abc.search(Q().a == 1))[0]
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
        entry = tuple(self.foo.all())[0]
        self.assertEqual(3, entry.longest())

    ####################################################################
    def test_additional_table_via_search(self):
        self.foo.insert({"short": "a", "medium": "aa", "long": "aaa"})
        entry = tuple(self.foo.search(Q().short == "a"))[0]
        self.assertEqual(3, entry.longest())

    ####################################################################
    def test_additional_table_via_index(self):
        self.foo.insert({"short": "a", "medium": "aa", "long": "aaa"})
        entry = tuple(self.foo.index("short"))[0]
        self.assertEqual(3, entry.longest())


########################################################################
class TestCreateDB(TestCase):
    """
    Tests to ensure creating, loading, and writing to database work for
    file-based dbs.
    """

    ####################################################################
    def setUp(self):
        data = {
            "ABC": {
                "1": {"a": 1}
            }
        }
        self.json_file = NamedTemporaryFile(suffix=".json", delete=False)
        self.json_file.write(json.dumps(data).encode("utf-8"))
        self.json_file.flush()
        self.json_file.close()

    ####################################################################
    def tearDown(self):
        os.unlink(self.json_file.name)

    ####################################################################
    def test_create_db_from_json(self):
        db = create_db(self.json_file.name, name="ABC", table=ABCTable)
        self.assertEqual(1, len(db))
        db.insert({"a": 2})
        with open(self.json_file.name) as f:
            data = json.loads(f.read())
        expected = {
            'ABC': {
                '1': {'a': 1},
                '2': {'a': 2},
            }
        }
        self.assertEqual(expected, data)

    ####################################################################
    def test_create_db_manually_from_json(self):
        db = TinyFatDB(self.json_file.name, default_table="ABC", table_class=ABCTable)
        self.assertEqual(1, len(db))
        db.insert({"a": 2})
        with open(self.json_file.name) as f:
            data = json.loads(f.read())
        expected = {
            'ABC': {
                '1': {'a': 1},
                '2': {'a': 2},
            }
        }
        self.assertEqual(expected, data)

    ####################################################################
    def test_create_db_manually(self):
        db = TinyFatDB(default_table="ABC", table_class=ABCTable, storage=MemoryStorage)
        table = db.table("ABC")
        self.assertTrue(isinstance(table, ABCTable))
        self.assertTrue(isinstance(db._storage, MemoryStorage))


########################################################################
class TestTinyFatQueryset(TestCase):

    ####################################################################
    def setUp(self):
        self.db = create_db()
        self.entries = ({"a": "A1", "b": "B1", "c": "C1"},
                        {"a": "A2", "b": "B2", "c": "C2"},
                        {"a": "A3", "b": "B3", "c": "C3"})
        eids = self.db.insert_multiple(self.entries)
        for entry in self.entries:
            entry.update({"eid": eids.pop(0)})

    ####################################################################
    def test_qty(self):
        self.db.remove(match_all_elements)

        queryset = self.db.all()
        self.assertEqual(0, queryset.qty())

        self.db.insert({})
        queryset = self.db.all()
        self.assertEqual(1, queryset.qty())

        self.db.insert({})
        queryset = self.db.all()
        self.assertEqual(2, queryset.qty())

    ####################################################################
    def test_refresh_from_db(self):
        queryset = self.db.all()
        self.assertEqual(self.entries, tuple(queryset))

        self.db.update({"a": "foo"}, eids=[self.db.first().eid])
        self.assertEqual(self.entries, tuple(queryset))

        queryset.refresh_from_db()
        self.assertEqual("foo", tuple(queryset)[0]["a"])

    ####################################################################
    def test_search(self):
        # Add an entry to get back 2 results
        entry = {"a": "AA2"}
        eid = self.db.insert(entry)
        new_element = self.db.get(eid=eid)
        all_queryset = self.db.all()

        # Search should produce queryset with existing entry ending in "2" and new entry
        queryset_1 = all_queryset.search(a_ends_with_2)
        elements = tuple(queryset_1)
        expected_elements = (self.entries[1], new_element)
        self.assertEqual(expected_elements, elements)

        # Modify new element
        self.db.update({"a": "foo"}, eids=[new_element.eid])
        # Search should still provide 2 entries because queryset is not updated
        queryset_2 = queryset_1.search(a_ends_with_2)
        elements = tuple(queryset_2)
        expected_elements = (self.entries[1], new_element)
        self.assertEqual(expected_elements, elements)

        # Refresh queryset
        queryset_2.refresh_from_db()
        # Should return only 1 entry now that the updated entry has been refreshed from the database
        elements = tuple(queryset_2)
        expected_elements = (self.entries[1], )
        self.assertEqual(expected_elements, elements)

    ####################################################################
    def test_first(self):
        queryset = self.db.all()
        self.assertEqual(self.entries[0], queryset.first())

    ####################################################################
    def test_values__no_elements(self):
        self.db.remove(match_all_elements)
        queryset = self.db.all()

        values = tuple(queryset.data("a"))
        self.assertEqual((), values)

        with self.assertRaises(ValueError):
            tuple(queryset.data())

    ####################################################################
    def test_values__no_args(self):
        queryset = self.db.all()
        with self.assertRaises(ValueError):
            tuple(queryset.data())

    ####################################################################
    def test_values__single_arg(self):
        queryset = self.db.all()
        expected_data = {
            1: {"a": "A1"},
            2: {"a": "A2"},
            3: {"a": "A3"}
        }
        self.assertEqual(expected_data, queryset.data("a"))

    ####################################################################
    def test_values__eid(self):
        queryset = self.db.all()
        expected_data = {
            1: {"eid": 1},
            2: {"eid": 2},
            3: {"eid": 3},
        }
        self.assertEqual(expected_data, queryset.data("eid"))

    ####################################################################
    def test_values__multiple_args(self):
        queryset = self.db.all()
        expected_data = {
            1: {"a": "A1", "c": "C1"},
            2: {"a": "A2", "c": "C2"},
            3: {"a": "A3", "c": "C3"}
        }
        self.assertEqual(expected_data, queryset.data("a", "c"))

    ####################################################################
    def test_values_list(self):
        queryset = self.db.all()
        values = queryset.values("a")
        self.assertEqual(("A1", "A2", "A3"), values)

    ####################################################################
    def test_values_list__missing_field(self):
        queryset = self.db.all()
        with self.assertRaises(KeyError):
            queryset.values("foo")
