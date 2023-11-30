import os
import sys
import unittest
from unittest.mock import patch, MagicMock
import psycopg2
import requests

# Add cwd to path
pwd = os.getcwd()
sys.path.append(pwd)

from web_scraper import connect_db, create_table, get_linkroots

# Test database variables
db_info = {'db_name':'test_database',
           'db_user':'postgres',
           'db_password':'postgres',
           'db_host':'test_db'}
table_info = {'name':'test_table',
              'columns':['ruler', 'mass'],
              'dtypes':['VARCHAR(30)', 'REAL']}

# Unit testing

# connect_db()
class TestDatabaseConnection(unittest.TestCase):

    @patch('web_scraper.psycopg2.connect')
    def test_connection_success(self, mock_connect):
        mock_connect.return_value = MagicMock(spec=psycopg2.extensions.connection)
        result = connect_db(**db_info)
        mock_connect.assert_called_once()
        self.assertIs(result, mock_connect.return_value)

    @patch('web_scraper.psycopg2.connect')
    def test_connection_failure(self, mock_connect):
        mock_connect.side_effect = psycopg2.Error
        result = connect_db(**db_info)
        mock_connect.assert_called_once()
        self.assertIsNone(result)

    def test_postgres_connection(self):
        with connect_db(**db_info) as result:
            self.assertIs(type(result), psycopg2.extensions.connection)

# create_table()
class TestTableCreation(unittest.TestCase):

    def setUp(self):
        # Test table info
        self.table_name = 'test_table'
        self.table_columns = ['id', 'name']
        self.dtypes = ['INTEGER', 'VARCHAR(30)']
        self.connection = connect_db(**db_info)

    @patch('web_scraper.psycopg2.connect')
    def test_create_table_success(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        create_table(mock_conn, self.table_name, self.table_columns, self.dtypes)
        
        expected_command = 'CREATE TABLE IF NOT EXISTS test_table (id INTEGER, name VARCHAR(30));'
        mock_cursor.execute.assert_called_with(expected_command)
        mock_cursor.close.assert_called()

    @patch('web_scraper.psycopg2.connect')
    def test_create_table_failure(self, mock_connect):
        mock_connect.return_value = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.cursor.return_value = mock_cursor

        mock_cursor.execute.side_effect = psycopg2.Error

        try:
            create_table(mock_connect, self.table_name, self.table_columns, self.dtypes)
            self.fail('psycopg2.Error not raised')
        except:
            pass

        mock_connect.cursor.assert_called_once()
        mock_cursor.execute.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    def test_create_table_outcome(self):
        create_table(self.connection, self.table_name, self.table_columns, self.dtypes)
        with self.connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM information_schema.tables WHERE table_name = '{self.table_name}';")
            result = cursor.fetchone()
        self.assertIsNotNone(result, f'{self.table_name} was not created')
        
    def tearDown(self):
        self.connection.close()

# get_linkroots()
class TestGetLinkRoots(unittest.TestCase):
     
    @patch('web_scraper.requests.get')
    def test_get_linkroots_success(self, mock_get):
        sample_html_path = 'tests/test_data/test_html/test_page_index.html'
        # Retrieve stored sample html
        with open(sample_html_path, 'r') as sample_html_file:
             sample_html = sample_html_file.read()

        mock_response = MagicMock()
        mock_response.content = sample_html.encode('utf-8')
        # Mocking __enter__ and __exit__ required for with statement in get_linkroots()
        mock_response.__enter__.return_value = mock_response
        mock_response.__exit__.return_value = None
        mock_get.return_value = mock_response

        test_url = 'http://testsite.com/coins/ric/'

        result = get_linkroots(test_url)

        expected_linkroots = ['http://testsite.com/coins/ric/agrippa/', 
                              'http://testsite.com/coins/ric/augustus/', 
                              'http://testsite.com/coins/ric/claudius/']
        
        self.assertEqual(result, expected_linkroots)
    
    @patch('web_scraper.requests.get')
    def test_get_linkroots_failure(self, mock_get):
        mock_get.side_effect = requests.ConnectionError
        

if __name__ == '__main__':
    unittest.main()