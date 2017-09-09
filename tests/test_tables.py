from .base import BaseTinyFatTest
from .utils import match_all_elements
from tinyfatdb import TinyFatDB, TinyFatTable
from tinydb import TinyDB
from tinydb import Query as Q

a_ends_with_2 = Q().a.test(lambda a: a.endswith("2"))


########################################################################
class TableAndModelTestSuite:
    _entries = ({"a": "A1", "b": "B1", "c": "C1"},
                {"a": "A2", "b": "B2", "c": "C2"},
                {"a": "A3", "b": "B3", "c": "C3"})

    ####################################################################
    def test_purge_tables(self):
        self.db.table("Foo", table=TinyFatTable)
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
class TestDefaultTableAndModel(BaseTinyFatTest, TableAndModelTestSuite):
    """
    Tests to ensure original TinyDB and TinyFatTable functions work
    with default TinyFatDB, TinyFatTable, and TinyFatModel classes.
    """

    ####################################################################
    def setUp(self):
        self.db = TinyFatDB.create()
        self.entries = self.insert_entries(self._entries)

    ####################################################################
    def test_add_table(self):
        self.db.table("Foo", table=TinyFatTable)
        self.assertEqual({'Foo', TinyDB.DEFAULT_TABLE}, self.db.tables())

    ####################################################################
    def test_purge_table(self):
        self.db.table("Foo", table=TinyFatTable)
        self.db.purge_table("Foo")
        self.assertEqual({TinyDB.DEFAULT_TABLE}, self.db.tables())


########################################################################
class TestCustomTableAndModel(BaseTinyFatTest, TableAndModelTestSuite):
    """
    Tests to ensure original TinyDB and TinyFatTable functions work
    with subclasses of TinyFatDB, TinyFatTable, and TinyFatModel classes.
    """

    ####################################################################
    def setUp(self):
        super(TestCustomTableAndModel, self).setUp()
        self.db = TinyFatDB.create(name="ABC", table=TinyFatTable)
        self.entries = self.insert_entries(self._entries)

    ####################################################################
    def test_add_table(self):
        self.db.table("Foo", table=TinyFatTable)
        self.assertEqual({"Foo", "ABC"}, self.db.tables())

    ####################################################################
    def test_purge_table(self):
        self.db.table("Foo", table=TinyFatTable)
        self.db.purge_table("Foo")
        self.assertEqual({"ABC"}, self.db.tables())
