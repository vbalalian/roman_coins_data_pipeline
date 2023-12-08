import os
import sys
import unittest
from unittest.mock import patch, MagicMock, call, mock_open
import psycopg2
import requests
from bs4 import BeautifulSoup
import datetime
# Add cwd to path
sys.path.append(os.getcwd())
from web_scraper import (connect_db, create_table, get_pages, scrape_page, 
                         pull_title, pull_subtitle, pull_coins, coin_id, 
                         coin_catalog, coin_description, coin_metal, coin_era, 
                         coin_year, coin_txt, coin_mass, coin_diameter, 
                         coin_inscriptions, coins_from_soup, load_coins, 
                         check_state, update_state, scrape_and_load, main)

# Test database variables
db_info = {'db_name':'test_database',
           'db_user':'postgres',
           'db_password':'postgres',
           'db_host':'test_db'}
table_info = {'name':'test_table',
              'columns':['ruler', 'mass'],
              'dtypes':['VARCHAR(30)', 'REAL']}

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
        self.table_name = table_info['name']
        self.table_columns = table_info['columns']
        self.dtypes = table_info['dtypes']

    def test_create_table_success(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.__exit__.return_value = None

        create_table(mock_conn, self.table_name, self.table_columns, self.dtypes)
        
        expected_command = 'CREATE TABLE IF NOT EXISTS test_table (ruler VARCHAR(30), mass REAL);'
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
        conn = connect_db(**db_info)
        create_table(conn, self.table_name, self.table_columns, self.dtypes)
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT * FROM information_schema.tables WHERE table_name = '{self.table_name}';")
            result = cursor.fetchone()
        conn.close()
        self.assertIsNotNone(result, f'{self.table_name} was not created')

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

# coin_id()
def test_coin_id():
    id = coin_id()
    assert len(id) == 36

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

# coin_catalog()
class TestCoinCatalog(unittest.TestCase):
    
    def test_normal_catalog(self):
        normal_coins = coins_from_html(path='tests/test_data/test_html/normal.html')
        self.assertEqual(coin_catalog(normal_coins[0]), 'TEST 123')
        self.assertEqual(coin_catalog(normal_coins[1]), 'TEST 124')
        self.assertEqual(coin_catalog(normal_coins[2]), 'TEST 125')

    def test_missing_catalog(self):
        missing_catalog_html_path = 'tests/test_data/test_html/missing_catalog.html'
        missing_catalog_coin = coins_from_html(path=missing_catalog_html_path)
        self.assertIsNone(coin_catalog(missing_catalog_coin))
        
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
        self.assertEqual(coin_mass(missing_mass_coin), None)

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
        self.assertEqual(coin_diameter(missing_diameter_coin), None)

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

# coins_from_soup()
class TestCoinsFromSoup(unittest.TestCase):

    def test_normal_soup(self):
        with open('tests/test_data/test_html/normal.html', 'r') as html_file:
            html = html_file.read()
        soup = BeautifulSoup(html.replace('\n', '').encode(), 'lxml')
        coins = coins_from_soup(soup)
        test_coin = coins[0]
        self.assertEqual(test_coin['ruler'], 'Test Ruler')
        self.assertEqual(test_coin['ruler_detail'], 'Test Subtitle')
        self.assertEqual(test_coin['catalog'], 'TEST 123')
        self.assertEqual(test_coin['description'], 'Test Description AD 350 28mm, 8.24g. AVG CAES filler')
        self.assertEqual(test_coin['metal'], 'Copper')
        self.assertEqual(test_coin['mass'], 8.24)
        self.assertEqual(test_coin['diameter'], 28.0)
        self.assertEqual(test_coin['era'], 'AD')
        self.assertEqual(test_coin['year'], 350)
        self.assertEqual(test_coin['inscriptions'], 'AVG,CAES')
        self.assertEqual(test_coin['txt'], 'https://www.wildwinds.com/coins/ric/test_ruler/TEST_123.txt')
        self.assertIsInstance(test_coin['created'], datetime.datetime)
        self.assertIsNone(test_coin['modified'])
        self.assertEqual(len(coins), 3)

# load_coins()
class TestLoadCoins(unittest.TestCase):

    def setUp(self):
        self.coins = [{'ruler': 'Test Ruler 1', 'mass': 0.0}, 
                      {'ruler': 'Test Ruler 2', 'mass': 2.4}]
        self.table_name = table_info['name']
        self.table_columns = table_info['columns']
        self.dtypes = table_info['dtypes']

    @patch('web_scraper.psycopg2.connect')
    def test_load_coins_success(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        load_coins(self.coins, mock_conn, self.table_name)

        expected_calls = [
            call(f"INSERT INTO {self.table_name} (ruler, mass) VALUES (%(ruler)s, %(mass)s);", coin)
            for coin in self.coins
        ]
        mock_cursor.execute.assert_has_calls(expected_calls, any_order=True)

        mock_conn.commit.assert_called()
        mock_cursor.close.assert_called()

    @patch('web_scraper.psycopg2.connect')
    def test_load_coins_failure(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        mock_cursor.execute.side_effect = psycopg2.Error("Test error")

        load_coins(self.coins, mock_conn, self.table_name)

        mock_conn.rollback.assert_called()

    def test_load_coins_outcome(self):
        conn = connect_db(**db_info)
        create_table(conn, self.table_name, self.table_columns, self.dtypes)
        load_coins(self.coins, conn, self.table_name, commit=False)
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {self.table_name};")
            result = cursor.fetchall()
        conn.close()

        expected_row_1 = {'ruler': 'Test Ruler 1', 'mass': 0.0}
        expected_row_2 = {'ruler': 'Test Ruler 2', 'mass': 2.4}
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], expected_row_1)
        self.assertEqual(result[1], expected_row_2)

# check_state()
class TestCheckState(unittest.TestCase):

    @patch('web_scraper.os.path.exists')
    def test_missing_state(self, mock_exists):
        mock_exists.return_value = False
        state = check_state('dummy_path.csv')
        self.assertIsNone(state)
    
    @patch('web_scraper.os.path.exists')
    @patch('web_scraper.open', new_callable=mock_open, read_data='Scraping State')
    def test_active_state(self, mock_file, mock_exists):
        mock_exists.return_value = True
        state = check_state('dummy_path.csv')
        self.assertEqual(state, 'Scraping State')
        
    @patch('web_scraper.os.path.exists')
    @patch('web_scraper.open', new_callable=mock_open, read_data='Scraping/Loading complete')
    @patch('web_scraper.exit')
    def test_complete_state(self, mock_exit, mock_file, mock_exists):
        mock_exists.return_value = True
        check_state('dummy_path.csv')
        mock_exit.assert_called_once()

# update_state()
class TestUpdateState(unittest.TestCase):

    @patch('web_scraper.open', new_callable=mock_open)
    def test_update_state(self, mock_open):
        test_input = "test_input"
        test_path = "test_state.csv"

        update_state(test_path, test_input)

        mock_open.assert_called_once_with(test_path, "a")
        mock_file = mock_open()
        mock_file.write.assert_called()
        args, _ = mock_file.write.call_args
        self.assertIn(test_input, args[0]) # Assert write called with test_input

# scrape_and_load()
class TestScrapeAndLoad(unittest.TestCase):

    @patch('web_scraper.scrape_page')
    @patch('web_scraper.coins_from_soup')
    @patch('web_scraper.load_coins')
    @patch('web_scraper.update_state')
    @patch('web_scraper.check_state')
    def test_scrape_and_load(self, mock_check_state, mock_update_state, mock_load_coins, mock_coins_from_soup, mock_scrape_page):
        mock_conn = MagicMock()
        mock_scrape_page.return_value = MagicMock()
        mock_coins_from_soup.return_value = [{'coin': 'data'}]

        state_path = '/path/to/statefile'
        pages = ['http://testurl.com/page1', 'http://testurl.com/page2']
        table_name = 'test_table'

        mock_check_state.side_effect = [None, 'http://testurl.com/page1']

        scrape_and_load(mock_conn, state_path, pages, table_name, delay=0)

        self.assertEqual(mock_scrape_page.call_count, 2)
        mock_load_coins.assert_called_with([{'coin': 'data'}], mock_conn, table_name)
        mock_update_state.assert_called() 

# main()
class TestMain(unittest.TestCase):

    @patch('web_scraper.get_pages')
    @patch('web_scraper.connect_db')
    @patch('web_scraper.create_table')
    @patch('web_scraper.scrape_and_load')
    def test_main(self, mock_scrape_and_load, mock_create_table, mock_connect_db, mock_get_pages):
        mock_get_pages.return_value = ['page1', 'page2', 'page3']
        mock_conn = MagicMock()
        mock_connect_db.return_value.__enter__.return_value = mock_conn

        test_db_info = db_info
        test_table_name = table_info['name']
        test_table_columns = table_info['columns']
        test_column_dtypes = table_info['dtypes']
        test_state_path = '/test/state/path'

        with patch('web_scraper.db_info', test_db_info), \
             patch('web_scraper.table_name', test_table_name), \
             patch('web_scraper.table_columns', test_table_columns), \
             patch('web_scraper.column_dtypes', test_column_dtypes), \
             patch('web_scraper.state_path', test_state_path):
            main()

        mock_get_pages.assert_called_with('https://www.wildwinds.com/coins/ric/i.html')
        mock_connect_db.assert_called_with(**test_db_info)
        mock_create_table.assert_called_with(mock_conn, test_table_name, test_table_columns, test_column_dtypes)
        mock_scrape_and_load.assert_called_with(mock_conn, test_state_path, ['page1', 'page2', 'page3'], test_table_name)

if __name__ == '__main__':
    unittest.main()