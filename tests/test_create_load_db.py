import unittest
import sqlite3
import os

import sys
print(sys.path)

from api.create_load_db import main

class TestCreateLoadDB(unittest.TestCase):

    def setUp(self):
        # Setup test environment
        self.test_db_path = 'test_roman_coins.db'
        # Additional setup if needed

    def test_database_creation(self):
        # Call the main function of your script to create the database and table
        main()
        
        # Connect to the test database
        con = sqlite3.connect(self.test_db_path)
        cur = con.cursor()
        
        # Check if the table exists
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='roman_coins';")
        self.assertIsNotNone(cur.fetchone())

        # Optionally, you can also check for the correct creation of columns
        cur.execute("PRAGMA table_info(roman_coins);")
        columns = [row[1] for row in cur.fetchall()]
        expected_columns = ['ruler', 'ruler_detail', 'id', 'description', 'metal', 'mass', 'diameter', 'era', 'year', 'inscriptions', 'txt']
        self.assertEqual(columns, expected_columns)

        # Close the connection
        con.close()

    def test_data_loading(self):
        # Call the main function of your script to create the database and load data
        main()

        # Connect to the test database
        con = sqlite3.connect(self.test_db_path)
        cur = con.cursor()

        # Verify that data is present in the table
        cur.execute("SELECT COUNT(*) FROM roman_coins;")
        count = cur.fetchone()[0]
        self.assertGreater(count, 0)

        # Optionally, test for specific data correctness
        # Example: Check if a specific record exists
        cur.execute("SELECT * FROM roman_coins WHERE id = 'some_id';")
        self.assertIsNotNone(cur.fetchone())

        # Close the connection
        con.close()
    
    def test_data_integrity(self):
        # Connect to the test database
        con = sqlite3.connect(self.test_db_path)
        cur = con.cursor()

        # Example: Check if numeric fields are correctly stored
        cur.execute("SELECT mass, diameter FROM roman_coins;")
        for mass, diameter in cur.fetchall():
            self.assertIsInstance(mass, float)
            self.assertIsInstance(diameter, float)

        # Close the connection
        con.close()

    def tearDown(self):
        # Clean up test environment
        if os.path.exists(self.test_db_path):
            os.remove(self.test_db_path)

if __name__ == '__main__':
    unittest.main()