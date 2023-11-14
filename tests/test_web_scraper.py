from web_scraping.web_scraper import (
    scrape, pull_title, pull_subtitle, pull_coins, coin_description, 
    coin_metal, coin_era, coin_year, coin_txt, coin_id, coin_mass, 
    coin_diameter, coin_inscriptions, coin_df, combine_coin_dfs, main
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

"""
# Test pull_coins
test_coins = pull_coins(test_soups[3])
print(f'{len(test_coins)} test coins. Raw data:\n')
pp.pprint(test_coins[:5])


# Function to test coin_description()
def test_coin_description(coins):
    descriptions = pd.Series([coin_description(coin) for coin in coins])
    print(f'Out of {len(coins)} coins, {descriptions.isna().sum()} are missing description value(s)')
    pp.pprint(descriptions.tolist()[:10])


# Function to test previous function coin_metal()
def test_coin_metal(coins):
    metals = pd.Series([coin_metal(coin) for coin in coins])
    print(f'Out of {len(coins)} coins, {metals.isna().sum()} are missing metal value(s)')
    pp.pprint(metals.tolist()[:25])


# Function to test coin_era() and coin_year()
def test_coin_era_and_year(coins):
    eras = [coin_era(coin) for coin in coins]
    years = [coin_year(coin) for coin in coins]
    df = pd.DataFrame({'eras':eras, 'years':years})
    print(f'Out of {len(coins)} coins:')
    print(f'{df.eras.isna().sum()} are missing era value(s).')
    print(f'{df.years.isna().sum()} are missing year value(s).')
    pp.pprint(eras[:25])
    pp.pprint(years[:25])


# Function to test previous function coin_metal()
def test_coin_txt(coins):
    txt = pd.Series([coin_txt(coin) for coin in coins])
    print(f'Out of {len(coins)} coins, {txt.isna().sum()} are missing .txt value(s)')
    pp.pprint(txt.tolist()[:25])


# Function to test previous function coin_id()
def test_coin_id(coins):
    id = pd.Series([coin_id(coin) for coin in coins])
    print(f'Out of {len(coins)} coins, {id.isna().sum()} are missing id value(s)')
    pp.pprint(id.tolist()[:25])



# Function to test coin_mass()
def test_coin_mass(coins):
    mass = pd.Series([coin_mass(coin) for coin in coins])
    print(f'Out of {len(coins)} coins, {mass.isna().sum()} are missing mass value(s)')
    pp.pprint(mass.tolist()[:25])

# Function to test coin_diameter()
def test_coin_diameter(coins):
    diameter = pd.Series([coin_diameter(coin) for coin in coins])
    print(f'Out of {len(coins)} coins, {diameter.isna().sum()} are missing diameter value(s)')
    pp.pprint(diameter.tolist()[:25])


def test_coin_inscriptions(coins):
    inscriptions = pd.Series([coin_inscriptions(coin) for coin in coins])
    print(f'Out of {len(coins)} coins, {inscriptions.isna().sum()} are missing inscriptions value(s)')
    pp.pprint(inscriptions.tolist()[:25])


# Combine helper functions for a basic summary of function success
def coin_tests(soups=test_soups):
    test_coins = []
    for s in soups:
        for c in pull_coins(s):
            test_coins.append(c)
    print(f'Out of {len(test_coins)} coins in {len(soups)} soups, there are:')
    descriptions = pd.Series([coin_description(coin) for coin in test_coins])
    print(f'  {descriptions.isna().sum()} missing description values')
    metals = pd.Series([coin_metal(coin) for coin in test_coins])
    print(f'  {metals.isna().sum()} missing metal values')
    print(f'    {metals.nunique()} unique metal values: {metals.unique()}')
    years = pd.Series([coin_year(coin) for coin in test_coins])
    print(f'  {years.isna().sum()} missing year values')
    print(f'    {years.nunique()} unique year values')
    print(f'      Mean: {years.mean()}, Median: {years.median()}, Min: {years.min()}, Max: {years.max()}')
    ids = pd.Series([coin_id(coin) for coin in test_coins])
    print(f'  {ids.isna().sum()} missing id values')
    print(f'    {ids.nunique()} unique id values')
    mass = pd.Series([coin_mass(coin) for coin in test_coins])
    print(f'  {mass.isna().sum()} missing mass values')
    print(f'    {mass.nunique()} unique mass values')
    print(f'      Mean: {mass.mean()}, Median: {mass.median()}, Min: {mass.min()}, Max: {mass.max()}')
    diameter = pd.Series([coin_diameter(coin) for coin in test_coins])
    print(f'  {diameter.isna().sum()} missing diameter values')
    print(f'    {diameter.nunique()} unique diameter values')
    print(f'      Mean: {diameter.mean()}, Median: {diameter.median()}, Min: {diameter.min()}, Max: {diameter.max()}')
    inscriptions = [coin_inscriptions(c) for c in test_coins]
    print(f'  {pd.Series(inscriptions).isna().sum()} missing inscriptions values')
    unique_inscriptions = list(set([item for inscription in inscriptions if inscription is not None for item in inscription]))
    print(f'    {len(unique_inscriptions)} unique inscriptions')
    print(f'      {unique_inscriptions}')
"""