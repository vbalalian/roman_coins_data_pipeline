from web_scraping.web_scraper import (
    scrape, pull_title, pull_subtitle, pull_coins, coin_id, coin_description, 
    coin_metal, coin_era, coin_year, coin_txt, coin_mass, coin_diameter, 
    coin_inscriptions, coin_df, combine_coin_dfs, main
)
from unittest.mock import patch, call, Mock
from bs4 import BeautifulSoup

@patch('requests.get')
def test_scrape(mock_get):
    # Create mock response object
    mock_response = Mock()
    mock_response.content = '<html><body>TEST HTML CONTENT</body></html>'
    mock_get.return_value = mock_response

    # Call scrape() function
    url_roots = ['http://testurl1.com/', 'http://testurl.com/2/']
    result = scrape(url_roots, delay=0)

    # Assertions
    expected_calls = [call('http://testurl1.com/i.html'), call('http://testurl.com/2/i.html')]
    mock_get.assert_has_calls(expected_calls)
    assert len(result) == len(url_roots)
    assert 'TEST HTML CONTENT' in result[0].content

def test_pull_title():
    # Normal case
    normal_html = '<html><head><title>Test Title</title></head><body></body></html>'
    assert pull_title(BeautifulSoup(normal_html, 'html.parser')) == 'Test Title'

    # Missing title
    missing_title_html = '<html><head></head><body></body></html>'
    assert pull_title(BeautifulSoup(missing_title_html, 'html.parser')) is None or ''

    # Empty title
    empty_title_html = '<html><head><title></title></head><body></body></html>'
    assert pull_title(BeautifulSoup(empty_title_html, 'html.parser')) == ''

def test_pull_subtitle():
    # Case with direct subtitle after <h2>
    direct_subtitle_html = '''
        <html><body><h2>Browsing Roman Imperial Coins of Test Ruler</h2>
        Subtitle Directly After H2 Tag</body></html>'''
    assert pull_subtitle(BeautifulSoup(direct_subtitle_html, 'html.parser')) == 'Subtitle Directly After H2 Tag'

    # Case with subtitle within <p> tag
    p_subtitle_html = '''
        <html><body><h2>Test Ruler</h2><p>Subtitle Within P Tag</p></body></html>'''
    assert pull_subtitle(BeautifulSoup(p_subtitle_html, 'html.parser')) == 'Subtitle Within P Tag'

    # Case with subtitle within <font> tag
    font_subtitle_html = '''
        <html><body><h2>Test Ruler</h2><p><font>Subtitle Within Font Tag</font></p></body></html>'''
    assert pull_subtitle(BeautifulSoup(font_subtitle_html, 'html.parser')) == 'Subtitle Within Font Tag'

    # Case with complex structure and valid subtitle
    complex_subtitle_html = '''
        <html><body><h2>Test Ruler</h2><p><img src="test.jpg"/><br/>Complex Subtitle Structure</p></body></html>'''
    assert pull_subtitle(BeautifulSoup(complex_subtitle_html, 'html.parser')) == 'Complex Subtitle Structure'

    # Case with no subtitle
    no_subtitle_html = '<html><body><h2>Test Ruler</h2><p></p></body></html>'
    assert pull_subtitle(BeautifulSoup(no_subtitle_html, 'html.parser')) is None

    # Case with navigation text (should be filtered out)
    navigation_text_html = '''
        <html><body><h2>Test Ruler</h2><p>Browse the Test Page</p></body></html>'''
    assert pull_subtitle(BeautifulSoup(navigation_text_html, 'html.parser')) is None

# Normal html and soup
normal_html = '''<html><body><br/><enter><p></p><h2></h2><p><br/></p><h3></h3>
<table><tr><td bgcolor="#b87333">TEST 123</td><td>Test Description 1</td><th>
<a href="TEST_123.txt">Text</a></th><td><a href="TEST_123.jpg">Image</a></td>
</tr><tr><td bgcolor="#C0C0C0">TEST 124</td><td>Test Description 2</td><th>
<a href="TEST_124.txt">Text</a></th><td><a href="TEST_124.jpg">Image</a></td>
</tr><tr><td bgcolor="#FFD700">TEST 125</td><td>Test Description 3</td><th>
<a href="TEST_125.txt">Text</a></th><td><a href="TEST_125.jpg">Image</a></td>
</tr></a></td></tr><tr><td></td></tr><tr><td></body></html>'''
normal_soup = BeautifulSoup(normal_html, 'html.parser')
normal_coins = [coin.contents for coin in normal_soup.find_all('tr') 
                if len(coin) >2 and 'bgcolor' in str(coin)]

def test_pull_coins():
    # Normal case
    normal_coins = pull_coins(normal_soup)
    assert len(normal_coins) == 3

    # Case with missing coins
    missing_coins_html = '''<html><body><br/><enter><p></p><h2></h2><p><br/>
    </p><h3></h3><table><tr><td></td></tr><tr><td></body></html>'''
    missing_coins_soup = BeautifulSoup(missing_coins_html, 'html.parser')
    missing_coins = pull_coins(missing_coins_soup)
    assert len(missing_coins) == 0

# Helper function
def coin_from_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    coins = pull_coins(soup)
    return coins[0]

def test_coin_id():
    # Normal case
    assert coin_id(normal_coins[0]) == 'TEST 123'
    assert coin_id(normal_coins[1]) == 'TEST 124'
    assert coin_id(normal_coins[2]) == 'TEST 125'

    # Case with missing id
    missing_id_html = '''<html><body><br/><enter><p></p><h2></h2><p><br/></p>
    <h3></h3><table><tr><td bgcolor="#b87333"></td><td>Test Description 1</td>
    <th><a href="TEST_123.txt">Text</a></th><td><a href="TEST_123.jpg">Image
    </a></td></tr><tr><td></td></tr><tr><td></body></html>'''
    missing_id_coin = coin_from_html(missing_id_html)
    assert coin_id(missing_id_coin) is None

def test_coin_description():
    # Normal case
    assert coin_description(normal_coins[0]) == 'Test Description 1'
    assert coin_description(normal_coins[1]) == 'Test Description 2'
    assert coin_description(normal_coins[2]) == 'Test Description 3'

    # Case with missing description
    missing_desc_html = '''<html><body><table><tr><td bgcolor="#B7A642">
    TEST 123</td><th><a href="TEST_123.txt">Text</a></th><td>
    <a href="TEST_123.jpg">Image</a></td></tr></body></html>'''
    missing_desc_coin = coin_from_html(missing_desc_html)
    assert coin_description(missing_desc_coin) is None

def test_coin_metal():
    # Normal case
    assert coin_metal(normal_coins[0]) == 'Copper'
    assert coin_metal(normal_coins[1]) == 'Silver'
    assert coin_metal(normal_coins[2]) == 'Gold'

    # Case with missing metal
    missing_metal_html = '''<html><body><table><tr><td>TEST 123</td><td>
    Test Description1</td><th><a href="TEST_123.txt">Text</a></th><td>
    <a href="TEST_123.jpg">Image</a></td></tr></body></html>'''
    missing_metal_soup = BeautifulSoup(missing_metal_html, 'html.parser')
    missing_metal_coins = pull_coins(missing_metal_soup)
    assert len(missing_metal_coins) == 0
    
def test_coin_era():
    # Case with era 'AD'
    era_AD_html = '''<html><body><br/><enter><p></p><h2></h2><p><br/></p><h3>
    </h3><table><tr><td bgcolor="#b87333">TEST 123</td><td>Test AD 24-36 filler 
    etc.</td><th><a href="TEST_123.txt">Text</a></th><td><a href="TEST_123.jpg">
    Image</a></td></tr><tr><td></td></tr><tr><td></body></html>'''
    era_AD_coin = coin_from_html(era_AD_html)
    assert coin_era(era_AD_coin) == 'AD'
    
    # Case with era 'BC'
    era_BC_html = '''<html><body><br/><enter><p></p><h2></h2><p><br/></p><h3>
    </h3><table><tr><td bgcolor="#b87333">TEST 123</td><td>Test 248-297 BC filler 
    etc.</td><th><a href="TEST_123.txt">Text</a></th><td><a href="TEST_123.jpg">
    Image</a></td></tr><tr><td></td></tr><tr><td></body></html>'''
    era_BC_coin = coin_from_html(era_BC_html)
    assert coin_era(era_BC_coin) == 'BC'

    # Case with no era
    missing_era_html = '''<html><body><br/><enter><p></p><h2></h2><p><br/></p><h3>
    </h3><table><tr><td bgcolor="#b87333">TEST 123</td><td>Test 248-297 filler 
    etc.</td><th><a href="TEST_123.txt">Text</a></th><td><a href="TEST_123.jpg">
    Image</a></td></tr><tr><td></td></tr><tr><td></body></html>'''
    missing_era_coin = coin_from_html(missing_era_html)
    assert coin_era(missing_era_coin) is None

def test_coin_year():
    # Case with one year
    single_year_html = '''<html><body><br/><enter><p></p><h2></h2><p><br/></p><h3>
    </h3><table><tr><td bgcolor="#b87333">TEST 123</td><td>Test 24 AD filler 
    etc.</td><th><a href="TEST_123.txt">Text</a></th><td><a href="TEST_123.jpg">
    Image</a></td></tr><tr><td></td></tr><tr><td></body></html>'''
    single_year_coin = coin_from_html(single_year_html)
    assert coin_year(single_year_coin) == 24

    # Case with multiple years
    multi_years_html = '''<html><body><br/><enter><p></p><h2></h2><p><br/></p><h3>
    </h3><table><tr><td bgcolor="#b87333">TEST 123</td><td>Test AD 248-297 filler 
    etc.</td><th><a href="TEST_123.txt">Text</a></th><td><a href="TEST_123.jpg">
    Image</a></td></tr><tr><td></td></tr><tr><td></body></html>'''
    multi_years_coin = coin_from_html(multi_years_html)
    assert coin_year(multi_years_coin) == 248

    # Case with BC (negative) years
    BC_years_html = '''<html><body><br/><enter><p></p><h2></h2><p><br/></p><h3>
    </h3><table><tr><td bgcolor="#b87333">TEST 123</td><td>Test 49-36 BC filler 
    etc.</td><th><a href="TEST_123.txt">Text</a></th><td><a href="TEST_123.jpg">
    Image</a></td></tr><tr><td></td></tr><tr><td></body></html>'''
    BC_years_coin = coin_from_html(BC_years_html)
    assert coin_year(BC_years_coin) == -49

    # Case with no years
    no_years_html = '''<html><body><br/><enter><p></p><h2></h2><p><br/></p><h3>
    </h3><table><tr><td bgcolor="#b87333">TEST 123</td><td>Test filler 
    etc.</td><th><a href="TEST_123.txt">Text</a></th><td><a href="TEST_123.jpg">
    Image</a></td></tr><tr><td></td></tr><tr><td></body></html>'''
    no_years_coin = coin_from_html(no_years_html)
    assert coin_year(no_years_coin) is None

# def test_coin_txt():
# def test_coin_mass():
# def test_coin_diameter(): 
# def test_coin_inscriptions():
# def test_coin_df():
# def test_combine_coin_dfs():
# def test_main():