import os
import sys
import unittest
from unittest.mock import patch, MagicMock
import psycopg2
import requests
from bs4 import BeautifulSoup
# Add cwd to path
sys.path.append(os.getcwd())
from web_scraper import (connect_db, create_table, get_pages, scrape_page, 
                         pull_title, pull_subtitle, pull_coins, coin_id,
                         coin_description, coin_metal, coin_era, coin_year,
                         coin_txt, coin_mass, coin_diameter, coin_inscriptions,
                         coins_from_soup, load_coins, check_state, 
                         update_state, scrape_and_load, main)

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
        mock_response.content = sample_html.encode()
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
        test_html_content = '<html><body><h1>TEST HTML CONTENT</h1></body></html>'
        mock_response = MagicMock()
        mock_response.content = test_html_content.encode()
        mock_get.return_value = mock_response

        test_url = 'http://testsite.com/coins/ric/augustus/i.html'

        result = scrape_page(test_url)

        mock_get.assert_called_with(test_url)
        self.assertIsInstance(result, BeautifulSoup)
        self.assertEqual(result, BeautifulSoup(test_html_content, 'lxml'))

    @patch('web_scraper.requests.get')
    def test_scrape_page_failure(self, mock_get):
        mock_get.side_effect = requests.ConnectionError

        test_url = 'http://testsite.com/coins/ric/augustus/i.html'

        with self.assertRaises(requests.ConnectionError):
            scrape_page(test_url)
        mock_get.assert_called_once()

# pull_title()
class TestPullTitle(unittest.TestCase):

    def setUp(self):
        self.normal_html = '<html><head><title>Test Title</title></head><body></body></html>'
        self.missing_title_html = '<html><head></head><body></body></html>'
        self.empty_title_html = '<html><head><title></title></head><body></body></html>'

    def test_normal_html(self):
        soup = BeautifulSoup(self.normal_html.encode(), 'lxml')
        response = pull_title(soup)
        expected_title = 'Test Title'
        self.assertEqual(response, expected_title)

    def test_missing_title_html(self):
        soup = BeautifulSoup(self.missing_title_html.encode(), 'lxml')
        response = pull_title(soup)
        self.assertIsNone(response)

    def test_empty_title_html(self):
        soup = BeautifulSoup(self.empty_title_html.encode(), 'lxml')
        response = pull_title(soup)
        self.assertIsNone(response)

# pull_subtitle()
class TestPullSubtitle(unittest.TestCase):

    def setUp(self):
        self.direct_subtitle_html = "<html><body><title>Test Ruler</title><h2>Browsing Coins of Test Ruler</h2>Subtitle</body></html>"
        self.p_subtitle_html = "<html><body><title>Test Ruler</title><h2></h2><p>Subtitle</p></body></html>"
        self.font_subtitle_html = "<html><body><h2>Test Ruler</h2><p><font>Subtitle</font></p></body></html>"
        self.complex_subtitle_html = '<html><body><h2>Test Ruler</h2><p><img src="test.jpg"/><br/>Subtitle</p></body></html>'
        self.no_subtitle_html = '<html><body><h2>Test Ruler</h2><p></p></body></html>'
        self.navigation_subtitle_html = '<html><body><h2>Test Ruler</h2><p>Browse the Test Page</p></body></html>'
        self.expected_subtitle = 'Subtitle'

    def test_direct_subtitle(self):
        soup = BeautifulSoup(self.direct_subtitle_html.encode(), 'lxml')
        response = pull_subtitle(soup)
        self.assertEqual(response, self.expected_subtitle)

    def test_p_subtitle(self):
        soup = BeautifulSoup(self.p_subtitle_html.encode(), 'lxml')
        response = pull_subtitle(soup)
        self.assertEqual(response, self.expected_subtitle)

    def test_font_subtitle(self):
        soup = BeautifulSoup(self.font_subtitle_html.encode(), 'lxml')
        response = pull_subtitle(soup)
        self.assertEqual(response, self.expected_subtitle)

    def test_complex_subtitle(self):
        soup = BeautifulSoup(self.complex_subtitle_html.encode(), 'lxml')
        response = pull_subtitle(soup)
        self.assertEqual(response, self.expected_subtitle)

    def test_no_subtitle(self):
        soup = BeautifulSoup(self.no_subtitle_html.encode(), 'lxml')
        response = pull_subtitle(soup)
        self.assertIsNone(response)

    def test_navigation_subtitle(self):
        soup = BeautifulSoup(self.navigation_subtitle_html.encode(), 'lxml')
        response = pull_subtitle(soup)
        self.assertIsNone(response)

# pull_coins()
class TestPullCoins(unittest.TestCase):

    def setUp(self):
        # Retrieve stored sample html
        normal_html_path = 'tests/test_data/test_html/normal.html'
        with open(normal_html_path, 'r') as normal_html_file:
             self.normal_html = normal_html_file.read()
        self.missing_coins_html = '<html><body><br/><enter><p></p><h2></h2><p><br/></p><h3></h3><table><tr><td></td></tr><tr><td></body></html>'

    def test_normal_coins(self):
        soup = BeautifulSoup(self.normal_html.encode(), 'lxml')
        response = pull_coins(soup)
        self.assertEqual(len(response), 3)
        self.assertEqual(response[0][0].get_text(), 'TEST 123')

    def test_missing_coins(self):
        soup = BeautifulSoup(self.missing_coins_html.encode(), 'lxml')
        response = pull_coins(soup)
        self.assertEqual(len(response), 0)

if __name__ == '__main__':
    unittest.main()