import io
import os
import json
from .base import BaseTinyFatTest
from tempfile import NamedTemporaryFile
from tinyfatdb import TinyFatDB, TinyFatTable
from tinydb.storages import MemoryStorage


########################################################################
class TestCreateDB(BaseTinyFatTest):
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
    def test_create_db__manual(self):
        db = TinyFatDB(default_table="ABC", table_class=TinyFatTable, storage=MemoryStorage)
        table = db.table("ABC")
        self.assertTrue(isinstance(table, TinyFatTable))
        self.assertTrue(isinstance(db._storage, MemoryStorage))

    ####################################################################
    def test_create_db__json(self):
        db = TinyFatDB.create(self.json_file.name, name="ABC", table=TinyFatTable)
        self.assertEqual(1, len(db))
        db.insert({"a": 2})
        with io.open(self.json_file.name) as f:
            data = json.loads(f.read())
        expected = {
            'ABC': {
                '1': {'a': 1},
                '2': {'a': 2},
            }
        }
        self.assertEqual(expected, data)
