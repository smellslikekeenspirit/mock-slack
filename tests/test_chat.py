import unittest
from src.chat import *
from src.swen344_db_utils import connect

class TestChat(unittest.TestCase):

    def test_build_tables(self):
        """Build the tables"""
        conn = connect()
        cur = conn.cursor()
        rebuildTables()
        cur.execute('SELECT * FROM example_table')
        self.assertEqual([], cur.fetchall(), "no rows in example_table")
        conn.close()

    def test_rebuild_tables_is_idempotent(self):
      """Drop and rebuild the tables twice"""
      rebuildTables()
      rebuildTables()
      conn = connect()
      cur = conn.cursor()
      cur.execute('SELECT * FROM example_table')
      self.assertEqual([], cur.fetchall(), "no rows in example_table")
      conn.close()
        