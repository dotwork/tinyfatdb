from .base import BaseTinyFatTest
from tinyfatdb import TinyFatDB, TinyFatTable
from tinydb import Query as Q


########################################################################
class TestFatModels(BaseTinyFatTest):
    """
    Tests to ensure custom methods on instances of TinyFatModel work,
    both for the default table and for additional tables.
    """

    ####################################################################
    def setUp(self):
        super(TestFatModels, self).setUp()
        self.db = TinyFatDB.create(name="ABC", table=TinyFatTable)
        self.table = self.db.table(name="Foo", table=TinyFatTable)
        self.insertable = {"a": 1, "b": 2, "c": 3}
        self.additional_insertable = {"short": "a", "medium": "aa", "long": "aaa"}

    ####################################################################
    def insert_entry(self, container, insertable):
        """
        Takes a container (db, or table) and returns a Model representation
        of the entry that was inserted.
        """
        
        eid = container.insert(insertable)
        expected_entry = {**insertable, "eid": eid}
        return expected_entry

    ####################################################################
    def test_default_table__get(self):
        expected_entry = self.insert_entry(self.db, self.insertable)
        actual_entry = self.db.get(eid=expected_entry["eid"])
        self.assertEqual(actual_entry, expected_entry)

    ####################################################################
    def test_default_table__first(self):
        expected_entry = self.insert_entry(self.db, self.insertable)
        actual_entry = self.db.first()
        self.assertEqual(actual_entry, expected_entry)

    ####################################################################
    def test_default_table__all(self):
        expected_entry = self.insert_entry(self.db, self.insertable)
        actual_entry = list(self.db.all())[0]
        self.assertEqual(actual_entry, expected_entry)

    ####################################################################
    def test_default_table__search(self):
        expected_entry = self.insert_entry(self.db, self.insertable)
        actual_entry = list(self.db.search(Q().a == 1))[0]
        self.assertEqual(actual_entry, expected_entry)

    ####################################################################
    def test_default_table__index(self):
        expected_entry = self.insert_entry(self.db, self.insertable)
        actual_entry = list(self.db.index("a"))[0]
        self.assertEqual(actual_entry, expected_entry)

    ####################################################################
    def test_additional_table__get(self):
        expected_entry = self.insert_entry(self.table, self.additional_insertable)
        actual_entry = self.table.get(eid=expected_entry["eid"])
        self.assertEqual(actual_entry, expected_entry)

    ####################################################################
    def test_additional_table__first(self):
        expected_entry = self.insert_entry(self.table, self.additional_insertable)
        actual_entry = self.table.first()
        self.assertEqual(actual_entry, expected_entry)

    ####################################################################
    def test_additional_table__all(self):
        expected_entry = self.insert_entry(self.table, self.additional_insertable)
        actual_entry = tuple(self.table.all())[0]
        self.assertEqual(actual_entry, expected_entry)

    ####################################################################
    def test_additional_table__search(self):
        expected_entry = self.insert_entry(self.table, self.additional_insertable)
        actual_entry = tuple(self.table.search(Q().short == "a"))[0]
        self.assertEqual(actual_entry, expected_entry)

    ####################################################################
    def test_additional_table__index(self):
        expected_entry = self.insert_entry(self.table, self.additional_insertable)
        actual_entry = tuple(self.table.index("short"))[0]
        self.assertEqual(actual_entry, expected_entry)
