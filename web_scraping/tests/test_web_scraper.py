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
    
    def test_normal_coins(self):
        # Retrieve stored sample html
        normal_html_path = 'tests/test_data/test_html/normal.html'
        with open(normal_html_path, 'r') as normal_html_file:
             normal_html = normal_html_file.read()
        soup = BeautifulSoup(normal_html.encode(), 'lxml')
        response = pull_coins(soup)
        self.assertEqual(len(response), 3)
        self.assertEqual(response[0][0].get_text(), 'TEST 123')

    def test_missing_coins(self):
        missing_coins_html = '<html><body><br/><enter><p></p><h2></h2><p><br/></p><h3></h3><table><tr><td></td></tr><tr><td></body></html>'
        soup = BeautifulSoup(missing_coins_html.encode(), 'lxml')
        response = pull_coins(soup)
        self.assertEqual(len(response), 0)

# Helper function
def coins_from_html(path:str = None, html:str = None):
    if path:
        with open(path, 'r') as html_file:
            html = html_file.read()
    soup = BeautifulSoup(html.replace('\n', '').encode(), 'lxml')
    coins = pull_coins(soup)
    if len(coins) == 1:
        return coins[0]
    return coins 

# coin_id()
class TestCoinID(unittest.TestCase):
    
    def test_normal_id(self):
        normal_coins = coins_from_html(path='tests/test_data/test_html/normal.html')
        self.assertEqual(coin_id(normal_coins[0]), 'TEST 123')
        self.assertEqual(coin_id(normal_coins[1]), 'TEST 124')
        self.assertEqual(coin_id(normal_coins[2]), 'TEST 125')

    def test_missing_id(self):
        missing_id_html_path = 'tests/test_data/test_html/missing_id.html'
        missing_id_coin = coins_from_html(path=missing_id_html_path)
        self.assertIsNone(coin_id(missing_id_coin))
        
# coin_description()
class TestCoinDescription(unittest.TestCase):

    def test_normal_descriptions(self):
        normal_coins = coins_from_html(path='tests/test_data/test_html/normal.html')
        self.assertEqual(coin_description(normal_coins[0]), 'Test Description AD 350 28mm, 8.24g. AVG CAES filler')
        self.assertEqual(coin_description(normal_coins[1]), 'Test Description 17 BC AE17mm,  filler. PON TR COS')
        self.assertEqual(coin_description(normal_coins[2]), 'Test Description 17-109 AD  Filler 8.4 g. PON TR P')

    def test_missing_description(self):
        missing_description_html = '<tr><td bgcolor="#B7A642">TEST 123</td><th><a href="TEST_123.txt">Text</a></th><td><a href="TEST_123.jpg">Image</a></td></tr>'
        missing_description_coin = coins_from_html(html=missing_description_html)
        self.assertIsNone(coin_description(missing_description_coin))

# coin_metal()
class TestCoinMetal(unittest.TestCase):

    def test_normal_metals(self):
        normal_coins = coins_from_html(path='tests/test_data/test_html/normal.html')
        self.assertEqual(coin_metal(normal_coins[0]), 'Copper')
        self.assertEqual(coin_metal(normal_coins[1]), 'Silver')
        self.assertEqual(coin_metal(normal_coins[2]), 'Gold')

    def test_missing_metal(self):
        missing_metal_html = '<tr><td>TEST 123</td><td>Test Desc</td><th><a href="TEST_123.txt">Text</a></th><td><a href="TEST_123.jpg">Image</a></td></tr>'
        missing_metal_coin = coins_from_html(html=missing_metal_html)
        self.assertIsNone(coin_metal(missing_metal_coin))
    
# coin_era()
class TestCoinEra(unittest.TestCase):

    def test_normal_eras(self):
        normal_coins = coins_from_html(path='tests/test_data/test_html/normal.html')
        self.assertEqual(coin_era(normal_coins[0]), 'AD')
        self.assertEqual(coin_era(normal_coins[1]), 'BC')

    def test_missing_era(self):
        missing_era_html = '<tr><td bgcolor="#b87333">TEST 123</td><td>Test Description 217-219 AE17mm, 8.4 g. PON TR P Filler</td><th><a href="TEST_123.txt">Text</a></th><td><a href="TEST_123.jpg">Image</a></td></tr>'
        missing_era_coin = coins_from_html(html=missing_era_html)
        self.assertIsNone(coin_era(missing_era_coin))

# coin_year()
class TestCoinYear(unittest.TestCase):

    def test_normal_years(self):
        normal_coins = coins_from_html(path='tests/test_data/test_html/normal.html')
        # Case with single year (350 AD)
        self.assertEqual(coin_year(normal_coins[0]), 350)
        # Case with single year (17 BC)
        self.assertEqual(coin_year(normal_coins[1]), -17)
        # Case with multiple years (17-109 AD)
        self.assertEqual(coin_year(normal_coins[2]), 17)
    
    def test_missing_year(self):
        missing_year_html = '<tr><td bgcolor="#b87333">TEST 123</td><td>Test Description  AE17mm, 8.4 g. PON TR P Filler filler</td><td><a href="TEST_123.txt">Text</a></td><td><a href="TEST_123.jpg">Image</a></td></tr>'
        missing_year_coin = coins_from_html(html=missing_year_html)
        self.assertIsNone(coin_year(missing_year_coin))

# coin_txt()
class TestCoinTxt(unittest.TestCase):

    def setUp(self):
        self.test_ruler = 'Test Ruler'
        self.test_url = 'https://www.wildwinds.com/coins/ric/test_ruler/TEST_123.txt'
    
    def test_normal_txt(self): # 4 elements
        normal_coins = coins_from_html(path='tests/test_data/test_html/normal.html')
        self.assertEqual(coin_txt(normal_coins[0], title=self.test_ruler), self.test_url)

    def test_txt_five_elements(self):
        # Case with 5 elements
        five_element_html = '<tr><td>Extra</td><td bgcolor="#FF0000">TEST 123</td><td>Test Description</td><td><a href="TEST_123.txt">txt file</a></td><td><a href="TEST_123.jpg">jpg file</a></td></tr>'
        five_element_coin = coins_from_html(html=five_element_html)
        self.assertEqual(coin_txt(five_element_coin, title=self.test_ruler), self.test_url)

    def test_txt_six_elements(self):
        # Case with 6 elements
        six_element_html = '<tr><td>Extra</td><td bgcolor="#FF0000">TEST 123</td><td>Test Description</td><td>Extra</td><td><a href="TEST_123.txt">txt file</a></td><td><a href="TEST_123.jpg">jpg file</a></td></tr>'
        six_element_coin = coins_from_html(html=six_element_html)
        self.assertEqual(coin_txt(six_element_coin, title=self.test_ruler), self.test_url)

    def test_txt_seven_elements(self):
        # Case with 7 elements
        seven_element_html = '<tr><td>Extra</td><td bgcolor="#FF0000">TEST 123</td><td>Extra</td><td>Test Description</td><td>Extra</td><td><a href="TEST_123.txt">txt file</a></td><td><a href="TEST_123.jpg">jpg file</a></td></tr>'
        seven_element_coin = coins_from_html(html=seven_element_html)
        self.assertEqual(coin_txt(seven_element_coin, title=self.test_ruler), self.test_url)

    def test_txt_eight_elements(self):
        # Case with 8 elements
        eight_element_html = '<tr><td>Extra</td><td bgcolor="#FF0000">TEST 123</td><td>Extra</td><td>Test Description</td><td>Extra</td><td><a href="TEST_123.txt">txt file</a></td><td>Extra</td><td><a href="123.jpg">jpg</a></td></tr>'
        eight_element_coin = coins_from_html(html=eight_element_html)
        self.assertEqual(coin_txt(eight_element_coin, title=self.test_ruler), self.test_url)

    def test_missing_txt(self):
        # Case with no txt
        no_txt_html = '<tr><td bgcolor="#FF0000">TEST 123</td><td>Test Desc</td><td>filler</td><td><a href="TEST_123.jpg">jpg file</a></td></tr>'
        no_txt_coin = coins_from_html(html=no_txt_html)
        self.assertIsNone(coin_txt(no_txt_coin, title=self.test_ruler))

# coin_mass()
class TestCoinMass(unittest.TestCase):

    def test_normal_mass(self):
        # Potential gram abbreviations
        gram_abs = ['16.71 g.', '27.23 g,', '24.8 g.', '8.32 g.', '3.65 gr.', 
                    '7.3 g.', '10-51 g.', '(3.80 gm).', '9.59g',]
        for ab in gram_abs:
            value = float(ab.split('g')[0].strip().replace('-', '.').replace('(', ''))
            ab_html = f'''<tr><td bgcolor="#FF0000">Id</td><td>Test Description Filler filler Filler filler {ab} filler</td><td><a href='123.txt'>txt</a></td><td><a href='123.jpg'>jpg</a></td></tr>'''
            ab_coin = coins_from_html(html=ab_html)
            self.assertEqual(coin_mass(ab_coin), value)

    def test_missing_mass(self):
        normal_coins = coins_from_html(path='tests/test_data/test_html/normal.html')
        missing_mass_coin = normal_coins[1]
        self.assertEqual(coin_mass(missing_mass_coin), 0.0)

# coin_diameter()
class TestCoinDiameter(unittest.TestCase):

    def test_normal_diameter(self):
        diameter_examples = {'28.4 mm':28.4, '29mm':29.0, 'AE28mm':28.0, '17 mm,':17.0, 
                            '18.33mm':18.33, '25.13mm,':25.13, '33 mm.':33.0}
        for ex in diameter_examples:
            value = diameter_examples[ex]
            ex_html = f'''<tr><td bgcolor="#FF0000">Id</td><td>Test Description Filler filler Filler filler {ex} filler</td><td><a href='123.txt'>txt</a></td><td><a href='123.jpg'>jpg</a></td></tr>'''
            coin = coins_from_html(html=ex_html)
            self.assertEqual(coin_diameter(coin), value)

    def test_missing_diameter(self):
        normal_coins = coins_from_html(path='tests/test_data/test_html/normal.html')
        missing_diameter_coin = normal_coins[2]
        self.assertEqual(coin_diameter(missing_diameter_coin), 0.0)

# coin_inscriptions()
class TestCoinInscriptions(unittest.TestCase):

    def test_normal_inscriptions(self):
        test_inscriptions = ['AVG', 'IMP CAES', 'GERM COS, CONSVL, PP', 
                            'PO PF SC CENS TPP', 'TR', 'RESTITVT', 'BRIT', 
                            'AVGVSTVS', 'CAESAR', 'C', 'TRIB POT', 'PON',
                            'MAX', 'PM', 'SPQR', 'S-C', 'TRP', 'PAX']
        for case in test_inscriptions:
            test_values = case.replace(',', '').split()
            case_html = f'''<tr><td bgcolor="#FF0000">Id</td><td>Test Description Filler filler Filler filler {case} filler</td><td><a href='123.txt'>txt</a></td><td><a href='123.jpg'>jpg</a></td></tr>'''
            coin = coins_from_html(html=case_html)
            self.assertEqual(set(coin_inscriptions(coin)), set(','.join(test_values)))
    
    def test_missing_inscription(self):
        # Case with no known inscriptions
        no_inscription_html = '''<tr><td bgcolor="#FF0000">Id</td><td>Test Description Filler filler Filler filler filler</td><td><a href='123.txt'>txt</a></td><td><a href='123.jpg'>jpg</a></td></tr>'''
        no_inscription_coin = coins_from_html(html=no_inscription_html)
        self.assertIsNone(coin_inscriptions(no_inscription_coin))

if __name__ == '__main__':
    unittest.main()