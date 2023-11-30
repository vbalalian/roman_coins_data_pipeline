import os
import sys
import unittest
from unittest.mock import patch, MagicMock
import psycopg2
import requests
# Add cwd to path
sys.path.append(os.getcwd())
from web_scraper import connect_db, create_table, get_pages, scrape_page

# Test database variables
db_info = {'db_name':'test_database',
           'db_user':'postgres',
           'db_password':'postgres',
           'db_host':'test_db'}
table_info = {'name':'test_table',
              'columns':['ruler', 'mass'],
              'dtypes':['VARCHAR(30)', 'REAL']}

# Unit tests
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
        with self.assertRaises(psycopg2.Error):
            connect_db(**db_info)
        mock_connect.assert_called_once()

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

    def test_create_table_success(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.__exit__.return_value = None

        create_table(mock_conn, self.table_name, self.table_columns, self.dtypes)
        
        expected_command = 'CREATE TABLE IF NOT EXISTS test_table (id INTEGER, name VARCHAR(30));'
        mock_cursor.execute.assert_called_with(expected_command)
        mock_cursor.__exit__.assert_called()

    def test_create_table_failure(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.__exit__.return_value = None
        mock_cursor.execute.side_effect = psycopg2.Error

        with self.assertRaises(psycopg2.Error):
            create_table(mock_conn, self.table_name, self.table_columns, self.dtypes)

        mock_conn.cursor.assert_called_once()
        mock_cursor.execute.assert_called_once()
    
    def test_create_table_outcome(self):
        create_table(self.connection, self.table_name, self.table_columns, self.dtypes)
        with self.connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM information_schema.tables WHERE table_name = '{self.table_name}';")
            result = cursor.fetchone()
        self.assertIsNotNone(result, f'{self.table_name} was not created')
        
    def tearDown(self):
        self.connection.close()

# get_pages()
class TestGetPages(unittest.TestCase):

    @patch('web_scraper.requests.get')
    def test_get_pages_success(self, mock_get):
        sample_html_path = 'tests/test_data/test_html/test_page_index.html'
        # Retrieve stored sample html
        with open(sample_html_path, 'r') as sample_html_file:
             sample_html = sample_html_file.read()

        mock_response = MagicMock()
        mock_response.content = sample_html.encode('utf-8')
        # Mocking __enter__ and __exit__ required for with statement in get_pages()
        mock_response.__enter__.return_value = mock_response
        mock_response.__exit__.return_value = None
        mock_get.return_value = mock_response

        test_url = 'http://testsite.com/coins/ric/i.html'

        result = get_pages(test_url)

        expected_pages = ['http://testsite.com/coins/ric/agrippa/i.html', 
                              'http://testsite.com/coins/ric/augustus/i.html', 
                              'http://testsite.com/coins/ric/claudius/i.html']
        
        self.assertEqual(result, expected_pages)
    
    @patch('web_scraper.requests.get')
    def test_get_pages_failure(self, mock_get):
        mock_get.side_effect = requests.exceptions.ConnectionError
        mock_get.__enter__.side_effect = mock_get
        mock_get.__exit__.side_effect = None

        test_url = 'http://testsite.com/coins/ric/i.html'

        with self.assertRaises(requests.ConnectionError):
            get_pages(test_url)
        mock_get.assert_called_once()

# scrape_page()
class TestScrapePage(unittest.TestCase):

    @patch('web_scraper.requests.get')
    def test_scrape_page_success(self, mock_get):
        mock_response = MagicMock()
        mock_response.content = '<html><body>TEST HTML CONTENT</body></html>'
        mock_get.return_value = mock_response

        test_url = 'http://testsite.com/coins/ric/augustus/i.html'

        result = scrape_page(test_url)

        mock_get.assert_called_once()
        self.assertIn('TEST HTML CONTENT', result)

    @patch('web_scraping.requests.get')
    def test_scrape_page_failure(self, mock_get):
        mock_get.side_effect = requests.ConnectionError

        test_url = 'http://testsite.com/coins/ric/augustus/i.html'

        with self.assertRaises(requests.ConnectionError):
            scrape_page(test_url)
        mock_get.assert_called_once()

if __name__ == '__main__':
    unittest.main()