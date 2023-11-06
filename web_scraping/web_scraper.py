#!/usr/bin/env python
# coding: utf-8

# ### Import libraries

# In[2]:


import pandas as pd
import random
from time import sleep
import pprint
import requests
from bs4 import BeautifulSoup
import re
pp = pprint.PrettyPrinter(compact=True)


# Scrape link directory
# Using BeautifulSoup, we'll scrape wildwinds.com/coins/ric/i.html for landing pages of coin subjects  in alphabetical order. As of 11/3/2023, wildwinds.com/robots.txt grants permission for webscraping requests, provided there is a delay of 30 seconds between each request, and that the webscraping software is not explicitly blacklisted.

with requests.get('https://www.wildwinds.com/coins/ric/i.html') as raw:
    soup = BeautifulSoup(raw.content, 'lxml')

# Parse html data for a clean list of ruler names

options = soup.find_all('option')
emperors_raw = [i.contents for i in options if i.attrs['value'] != ''][:-6]
emperors = []
for line in emperors_raw:
    for text in line:
        emperors.append(text.strip())
print(f'First five: {emperors[:5]} \nLast five: {emperors[-5:]} \n{len(emperors)} emperors total')


# Generate list of usable link roots for each Emperor's coin page

# wildwinds.com/robots.txt requires a 30-second delay between requests
linkroots = ['https://www.wildwinds.com/coins/ric/' + i.attrs['value'][:-6] for i in options if i.attrs['value'] != ''][:-6]
print(f'First five: {linkroots[:5]} \nLast five: {linkroots[-5:]} \n{len(linkroots)} linkroots total')

# Create a semi-random list of test pages for building an adaptable parser

test_ids = random.choices(range(len(linkroots)), k=10) # 10 random index numbers
test_roots = [linkroots[i] for i in test_ids] # Corresponding url roots
for root in test_roots:
    print(root)

# pull html from test pages

# Define function that scrapes indicated pages with 30-second delay between requests
def scrape(url_roots: list[str]):
    combined_pages_html = []
    max_length = 0
    for root in url_roots:
        url = root + 'i.html'
        message = f'requesting {url} ({url_roots.index(root) + 1}/{len(url_roots)})'
        max_length = max(max_length, len(message))
        print(f'\r{message.ljust(max_length)}', end=' ', flush=True)
        sleep(30)
        page_html = requests.get(url)
        combined_pages_html.append(page_html)
    # After the loop, print a final message that clears the last line
    print(f'\rscraping complete: {url_roots.index(root) + 1}/{len(url_roots)}'.ljust(max_length))
    return combined_pages_html

# Scrape test pages' html (this takes some time with the delay)
test_pages = scrape(test_roots)

# Inspect test page html

# Convert page html into easier-to-manipulate format using BeautifulSoup
test_soups = [BeautifulSoup(page.content, 'lxml') for page in test_pages]

print(f'Test soup preview: \n\n{str(test_soups[5])[:2000]}')


# Create helper functions for parsing data fields

# Define function for parsing names of coin subjects
def pull_title(soup):
    raw_title = soup.find('title').text
    sep_index = raw_title.find(',')
    if sep_index == -1:
        sep_index = raw_title.find('-')
    return raw_title[:sep_index].strip() if sep_index != -1 else raw_title.strip()

# Function to test pull_title()
def test_pull_title(soups): 
    titles = pd.Series([pull_title(soup) for soup in soups])
    print(f'Out of {len(soups)} soups, {titles.isna().sum()} have missing titles.')
    pp.pprint(titles.tolist()[:20])

test_pull_title(test_soups)

# Function to pull subtitles
def pull_subtitle(soup):
    possible_locations = [
        lambda s: s.find_all('h3')[0].contents[-1],
        lambda s: s.find('font').contents[0],
        lambda s: s.find_all('p')[1].contents[-1],
        lambda s: s.find_all('br')[0].contents[0],
    ]
    
    for get_subtitle in possible_locations:
        try:
            subtitle = get_subtitle(soup)
            if not subtitle or len(str(subtitle)) < 4:
                continue
            if any(keyword in str(subtitle) for keyword in ['Click', 'Browse']):
                continue
            if '(' in str(subtitle) or '<' in str(subtitle):
                return None
            return str(subtitle).strip()
        except (IndexError, AttributeError):
            continue 
    
    return None

# Function to test pull_subtitle()
def test_pull_subtitle(soups):
    subtitles = pd.Series([pull_subtitle(soup) for soup in soups])
    print(f'Out of {len(soups)} soups, {subtitles.isna().sum()} are missing subtitles.')
    pp.pprint(subtitles.tolist()[:15]) 

test_pull_subtitle(test_soups)

# Function to pull raw coin data
def pull_coins(soup):
    coins = [coin.contents for coin in soup.find_all('tr') if len(coin) >2 and 'bgcolor' in str(coin)]
    return coins

# Test pull_coins
test_coins = pull_coins(test_soups[3])
print(f'{len(test_coins)} test coins. Raw data:\n')
pp.pprint(test_coins[:5])

# Function to pull coin descriptions
def coin_description(coin):
    try:
        description_html = str(coin[1])
        match = re.search(r'<td[^>]*>([^<]+)</td>', description_html)
        if match:
            description = match.group(1)  # The captured group from the regex
            return description.strip()
    except IndexError:
        return None

# Function to test coin_description()
def test_coin_description(coins):
    descriptions = pd.Series([coin_description(coin) for coin in coins])
    print(f'Out of {len(coins)} coins, {descriptions.isna().sum()} are missing description value(s)')
    pp.pprint(descriptions.tolist()[:10])

test_coin_description(test_coins)

# Function to identify coin metal
def coin_metal(coin):
    metals = {'#B8':'Copper','#b8':'Copper', '#FF':'Gold', '#C0':'Silver', '#B7':'Brass', '#b7':'Brass', 'red':'FAKE'}
    coin = str(coin)
    try:
        bg_color_index = int(coin.find('bgcolor=')) + 9
        bg_color = coin[bg_color_index:bg_color_index + 3]
        metal = metals[bg_color]
    except:
        return None
    return metal

# Function to test previous function coin_metal()
def test_coin_metal(coins):
    metals = pd.Series([coin_metal(coin) for coin in coins])
    print(f'Out of {len(coins)} coins, {metals.isna().sum()} are missing metal value(s)')
    pp.pprint(metals.tolist()[:25])

test_coin_metal(test_coins)

# Function to pull coin era (i.e. 'AD' or 'BC') 
def coin_era(coin):
    match = re.search(r'\b(AD|BC)\b', str(coin))
    return match.group(0) if match else None

# Function to pull a year (not *every* year) in the coin description
# (if there is a range of years i.e. 117-124 AD, function pulls the year closest to era i.e. '117-124 AD' returns '124', while 'AD 117-124' returns '117')
def coin_year(coin):
    era = coin_era(coin)
    if not era:
        return None

    if era == 'AD':
        # Look for the year pattern before 'AD'
        match = re.search(r'(\d{1,4})(?=\s*AD)', str(coin))
    else:
        # Look for the year pattern before 'BC'
        match = re.search(r'(\d{1,4})(?=\s*BC)', str(coin))

    if not match:
        # If no year is found before the era, search after it
        match = re.search(r'(?<=\bAD\s)(\d{1,4})', str(coin)) if era == 'AD' else re.search(r'(?<=\bBC\s)(\d{1,4})', str(coin))

    if match:
        year = int(match.group(0))
        return year if era == 'AD' else -year
    else:
        return None

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

test_coin_era_and_year(test_coins)

# wildwinds.com/robots.txt forbids use of their images, so those urls will not be parsed

# Function to pull .txt urls
def coin_txt(coin):
    for item in coin:
        match = re.search(r'href="([^"]+\.txt)"', str(item))
        if match:
            return match.group(1)

# Function to test previous function coin_metal()
def test_coin_txt(coins):
    txt = pd.Series([coin_txt(coin) for coin in coins])
    print(f'Out of {len(coins)} coins, {txt.isna().sum()} are missing .txt value(s)')
    pp.pprint(txt.tolist()[:25])

test_coin_txt(test_coins)

# Function to pull coin ids from jpg or txt urls
def coin_id(coin):
    coin = str(coin)
    match = re.search(r'href="_*([^"]+?)\.(jpg|txt)"', coin)
    if match:
        return match.group(1)

# Function to test previous function coin_id()
def test_coin_id(coins):
    id = pd.Series([coin_id(coin) for coin in coins])
    print(f'Out of {len(coins)} coins, {id.isna().sum()} are missing id value(s)')
    pp.pprint(id.tolist()[:25])

test_coin_id(test_coins)

# Function to pull coin mass (in grams)
def coin_mass(coin):
    coin = str(coin)
    gram_variations = [r'\bgr\b', r'\bgm\b', r'\bg\b'] 
    
    def extract_mass(pattern, coin_text):
        match = re.search(r'(\d+(?:\.\d+)?)\s*' + pattern, coin_text)
        if match:
            num_str = match.group(1).replace(',', '.')
            try:
                return float(num_str)
            except ValueError:
                return None
        return None

    for grams in gram_variations:
        mass = extract_mass(grams, coin)
        if mass is not None:
            return mass
            
    return None

# Function to test coin_mass()
def test_coin_mass(coins):
    mass = pd.Series([coin_mass(coin) for coin in coins])
    print(f'Out of {len(coins)} coins, {mass.isna().sum()} are missing mass value(s)')
    pp.pprint(mass.tolist()[:25])

test_coin_mass(test_coins)

# Function to pull coin diameter (in mm)
def coin_diameter(coin):
    coin = str(coin)
    pattern = re.compile(r'(\d+(\.\d*)?)\s*mm')

    match = pattern.search(coin)
    
    if match:
        return float(match.group(1))
    
    return None

# Function to test coin_diameter()
def test_coin_diameter(coins):
    diameter = pd.Series([coin_diameter(coin) for coin in coins])
    print(f'Out of {len(coins)} coins, {diameter.isna().sum()} are missing diameter value(s)')
    pp.pprint(diameter.tolist()[:25])

test_coin_diameter(test_coins)


# Check for common inscriptions
# ...such as "AVG" (Augustus, title of the emperor), "IMP" (Imperator (victorious general), received upon accession), "CAES" (Caesar, inherited name of the Julian family (Julius Caesar), used by later emperors to designate heir), "GERM" (Germanicus, a title honoring military victories in Germany), "COS" or "CONSVL" (Consul, a title linked to highest office in Senate, usually held by emperor), "PO" (Pontifex Maximus, highest priest, the head of state religion), "PP" (Pater Patriae, father of the country), "PF" (Pius Felix, reverent or dutiful), "SC" (Senatus Consultus), "TPP" (Tribunica Potestate, tribune of the people, each renewal indicated by numerals), "CENS" (Censor, a public office overseeing taxes, morality, the census and membership in various orders), "BRIT" (Britannicus).

# Function to pull recognized inscriptions
def coin_inscriptions(coin):
    coin = f" {str(coin)} "  # Pad with spaces to ensure we can match inscriptions at the edges
    inscriptions_list = ['AVG', 'IMP', 'CAES', 'GERM', 'COS', 'CONSVL', 'PP', 'PO', 'PF',
                         'SC', 'CENS', 'TPP', 'TR', 'RESTITVT', 'BRIT', 'AVGVSTVS', 'CAESAR',
                         'C', 'TRIB POT', 'PON MAX', 'PM']
    found_inscriptions = [i for i in inscriptions_list if f' {i} ' in coin]
    unique_inscriptions = list(set(found_inscriptions))
    return unique_inscriptions if unique_inscriptions else None

def test_coin_inscriptions(coins):
    inscriptions = pd.Series([coin_inscriptions(coin) for coin in coins])
    print(f'Out of {len(coins)} coins, {inscriptions.isna().sum()} are missing inscriptions value(s)')
    pp.pprint(inscriptions.tolist()[:25])

test_coin_inscriptions(test_coins)

# Create coin_tests() for a basic summary of function success

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

coin_tests()

# Function that combines previous helper functions to return coin DataFrame
def coin_df(soup):
    title = pull_title(soup)
    subtitle = pull_subtitle(soup)
    id, description, metal, mass, diameter, era, year, inscriptions, txt = [], [], [], [], [], [], [], [], []
    for coin in pull_coins(soup):
        id.append(coin_id(coin))
        description.append(coin_description(coin))
        metal.append(coin_metal(coin))
        mass.append(coin_mass(coin))
        diameter.append(coin_diameter(coin))
        era.append(coin_era(coin))
        year.append(coin_year(coin))
        inscriptions.append(coin_inscriptions(coin))
        txt.append(coin_txt(coin))
    return pd.DataFrame({'ruler':title, 'ruler_detail':subtitle, 'id':id, 'description':description, 'metal':metal, 'mass':mass, \
                        'diameter':diameter, 'era':era, 'year':year, 'inscriptions':inscriptions, 'txt':txt})

coin_df(test_soups[2]).head()

# Function to combine multiple coin Dataframes
def combine_coin_dfs(soups):
    dfs = [coin_df(soup) for soup in soups]
    return pd.concat(dfs, ignore_index=True) 

len(combine_coin_dfs(test_soups))


# Pull html from all source pages
# (pulling from over 200 pages, which takes a couple hours with the 30 second delay between requests)

all_pages = scrape(linkroots)

all_soups = [BeautifulSoup(page.content, 'lxml') for page in all_pages]


# Run tests

test_pull_title(all_soups)
test_pull_subtitle(all_soups)
coin_tests(all_soups)


# Combine it all into a single Dataframe

roman_coins_raw = combine_coin_dfs(all_soups)


#Check data quality

roman_coins_raw.head(10)

roman_coins_raw.info()

roman_coins_raw.describe()

roman_coins = roman_coins_raw.drop_duplicates(subset=['id'], keep='first')

roman_coins['metal'].fillna('None')
roman_coins['era'].fillna('None')
metal_categories = ['None', 'FAKE', 'Brass', 'Copper', 'Silver', 'Gold']
roman_coins['metal'] = pd.Categorical(roman_coins['metal'], categories=metal_categories, ordered=True)
era_categories = ['None', 'BC', 'AD']
roman_coins['era'] = pd.Categorical(roman_coins['era'], categories=era_categories, ordered=True)

# Export roman_coins DataFrame as csv
roman_coins.to_csv('roman_coins.csv', index=False)

roman_coins.info()

roman_coins.describe()