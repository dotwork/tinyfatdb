from .base import BaseTinyFatTest
from .utils import match_all_elements
from tinyfatdb import TinyFatDB
from tinydb import Query as Q


########################################################################
class TestTinyFatQueryset(BaseTinyFatTest):

    ####################################################################
    def setUp(self):
        self.db = TinyFatDB.create()
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
        a_ends_with_2 = Q().a.test(lambda a: a.endswith("2"))

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
