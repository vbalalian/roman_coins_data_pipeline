import unittest
import sqlite3
from api.create_load_db import connect_db, create_table, load_table, main

mock_data = [{'ruler':'Test Ruler 1', 'ruler_detail':'Test Detail 1', 'id':'Test ID 1', 'description':'Test Description 1', 'metal': 'Metal1', 'mass':0.0, 'diameter':1.0, 'era':'AD', 'year':100, 'inscriptions':'AVG', 'txt':'http://www.wildwinds.com/coins/ric/test_ruler_1/i.html'},
             {'ruler':'Test Ruler 2', 'ruler_detail':'Test Detail 2', 'id':'Test ID 2', 'description':'Test Description 2', 'metal': 'Metal2', 'mass':0.0, 'diameter':2.0, 'era':'AD', 'year':200, 'inscriptions':'AVG', 'txt':'http://www.wildwinds.com/coins/ric/test_ruler_2/i.html'},
             {'ruler':'Test Ruler 3', 'ruler_detail':'Test Detail 3', 'id':'Test ID 3', 'description':'Test Description 3', 'metal': 'Metal3', 'mass':0.0, 'diameter':3.0, 'era':'AD', 'year':300, 'inscriptions':'AVG', 'txt':'http://www.wildwinds.com/coins/ric/test_ruler_3/i.html'}]
mock_dtypes = ['TEXT', 'TEXT', 'TEXT', 'TEXT', 'TEXT', 'REAL', 'REAL', 'TEXT', 'INTEGER', 'TEXT', 'TEXT']

class TestCreateLoadDB(unittest.TestCase):

    def setUp(self):
        # Set up test environment
        self.connection = connect_db(':memory:')
        self.table = 'test_table'
        self.columns = list(mock_data[0].keys())
        self.dtypes = mock_dtypes
        self.data_rows = mock_data
    
    def test_connect_db(self):
        # Test helper function that handles db connection
        con = self.connection
        self.assertIsInstance(con, sqlite3.Connection)

    def test_create_and_load(self):
        # Call the function to create test table
        create_table(self.connection, self.table, self.columns, self.dtypes)

        # Connect to the test database
        con = self.connection
        cur = con.cursor()

        # Check if the table exists
        cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self.table}';")
        self.assertIsNotNone(cur.fetchone())

        # Check for the correct creation of columns
        cur.execute(f"PRAGMA table_info({self.table});")
        results = cur.fetchall()
        columns = [row[1] for row in results]
        dtypes = [row[2] for row in results]
        expected_columns = self.columns
        expected_dtypes = self.dtypes
        self.assertEqual(columns, expected_columns)
        self.assertEqual(dtypes, expected_dtypes)

        # Call the function to load test rows
        load_table(self.connection, self.table, self.data_rows)

        # Verify that data is present in the table
        cur.execute(f"SELECT COUNT(*) FROM {self.table};")
        count = cur.fetchone()[0]
        self.assertGreater(count, 0)

        # Check if a specific record exists
        cur.execute(f"SELECT * FROM {self.table} WHERE id = 'Test ID 3';")
        self.assertIsNotNone(cur.fetchone())

    def tearDown(self):
        # Close the in-memory db
        self.connection.close()

if __name__ == '__main__':
    unittest.main()