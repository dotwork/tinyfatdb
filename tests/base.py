from unittest import TestCase


########################################################################
class BaseTinyFatTest(TestCase):
    ####################################################################
    def insert_entries(self, entries):
        eids = self.db.insert_multiple(entries)
        for entry in entries:
            entry.update({"eid": eids.pop(0)})
        return entries
