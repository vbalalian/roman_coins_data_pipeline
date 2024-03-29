#!/usr/bin/env python
# coding: utf-8

from time import sleep
import requests
from bs4 import BeautifulSoup
import re
import os
import csv
import psycopg2
from psycopg2.extras import RealDictCursor
import datetime
import uuid

db_info = {'db_name':os.getenv('DB_NAME', 'roman_coins'),
           'db_user':os.getenv('DB_USER', 'postgres'),
           'db_password':os.getenv('DB_PASSWORD', 'postgres'),
           'db_host':'db'}

table_name = 'roman_coins'
table_columns = ['id', 'name', 'name_detail', 'catalog', 'description', 
                 'metal', 'mass', 'diameter', 'era', 'year', 'inscriptions', 
                 'txt', 'created', 'modified']
column_dtypes = [
    'VARCHAR(50) PRIMARY KEY', 'VARCHAR(30)', 'VARCHAR(1000)', 'VARCHAR(80)', 'VARCHAR(1000)', 
    'VARCHAR(20)', 'REAL', 'REAL', 'VARCHAR(5)', 'INTEGER', 'VARCHAR(100)', 
    'VARCHAR(105)', 'TIMESTAMP', 'TIMESTAMP'
    ]

state_path = '/app/data/scraping_state.csv'

def connect_db(db_name, db_user, db_password, db_host):
    '''Returns a connection with PostgreSQL db at path'''
    conn = psycopg2.connect(
        dbname=db_name, 
        user=db_user, 
        password=db_password, 
        host=db_host,
        cursor_factory=RealDictCursor)
    return conn
    

def create_table(conn:psycopg2.extensions.connection, table:str, cols:list, dtypes:list):
    '''Creates a table based on input connection & parameters'''
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE IF NOT EXISTS {table} (' + 
                    ', '.join(f'{col} {dtype}' for col, dtype 
                                in zip(cols, dtypes)) + ');')

def get_pages(directory_url:str):
    '''Scrapes directory for a list of coin figurehead pages'''
    with requests.get(directory_url) as html:
        soup = BeautifulSoup(html.content, 'lxml')
    pages = []
    root = directory_url[:-6]
    for element in soup.find_all("tr"):
        branch = element.find("a", href=True)
        if branch and "/i.html" in branch["href"]:
            pages.append(root + branch["href"])
    return pages

def scrape_page(url: str):
    '''Returns BeautifulSoup object of url'''
    with requests.get(url) as html:
        soup = BeautifulSoup(html.content, 'lxml')
    return soup

def pull_title(soup):
    '''Returns title element (corresponding to name) from BeautifulSoup object'''
    raw_title = soup.find('title')
    if raw_title:
        title_text = raw_title.text
        sep_index = title_text.find(',')
        if sep_index == -1:
            sep_index = title_text.find('-')
        title = title_text[:sep_index].strip() if sep_index != -1 else title_text.strip()
        if title:
            return title
    return None

def pull_subtitle(soup):
    '''Returns subtitle element from BeautifulSoup object'''
    # Filter function to exclude unwanted text
    def valid_subtitle(text):
        if not text or len(text) < 4:
            return False
        unwanted_phrases = ['Click', 'Browse', 'Main Page', 'RPC', 'RIC', 'Sear', 
                            'NOTE:', 'Note:', 'NOW', 'NEW', 'example', 'examples', 
                            'RSC']
        if any(phrase in text for phrase in unwanted_phrases):
            return False
        return True

    # Check directly after <h2> tags
    h2_subtitle = soup.find('h2')
    if h2_subtitle and h2_subtitle.next_sibling and h2_subtitle.next_sibling.string:
        subtitle = h2_subtitle.next_sibling.string.strip()
        if valid_subtitle(subtitle):
            return subtitle

    # Check within <p> tags for subtitles
    for p_tag in soup.find_all('p'):
        p_text = p_tag.get_text(strip=True)
        if valid_subtitle(p_text):
            return p_text

    # Fallback: Check within <font> tags
    font_tag = soup.find('font')
    if font_tag:
        font_text = font_tag.get_text(strip=True)
        if valid_subtitle(font_text):
            return font_text

    return None

def pull_coins(soup):
    '''Returns coins as raw BeautifulSoup objects'''
    coins = [coin.contents for coin in soup.find_all('tr') if len(coin) >2 and 'bgcolor' in str(coin)]
    return coins

def coin_id():
    return str(uuid.uuid4())

def coin_catalog(coin):
    '''Returns catalog information (str) from individual coin (BeautifulSoup) object'''
    try:
        catalog_html = coin[0]
        catalog = catalog_html.get_text()
        return catalog if catalog else None
    except IndexError:
        return None

def coin_description(coin):
    '''Returns description (str) from coin (BeautifulSoup) object'''
    try:
        for i in range(1, 4):
            chunk = coin[i]
            desc = chunk.get_text().replace('\r\n', '')
            if len(desc) >= 50:
                return desc
    except IndexError:
        return None

def coin_metal(coin):
    '''Returns metal/material (str) from coin (BeautifulSoup) object'''
    metals = {
        '#B87334':'Copper', '#B87333':'Copper', '#b87333':'Copper', 
        '#FFD700':'Gold', '#ffd700':'Gold', '#FFD660':'Gold', '#FFC601':'Gold', 
        '#C0C0C0':'Silver', '#c0C0C0':'Silver', '#c0c0c0':'Silver', '#cc9966':'Silver',
        '#B7A642':'Bronze', '#b7a642':'Bronze', 
        '#AC9B88':'Lead', 
        '#FFF7A3':'Bone',
        'red':'FAKE', '#F34629':'FAKE', '#FA6262':'FAKE', '#FF0000':'FAKE', '#FF4040':'FAKE', '#FA6036':'FAKE'}
    try:
        color = list(coin[0].attrs.values())[0]
        return metals[color]
    except:
        return None

def coin_era(coin):
    '''Returns era (i.e. 'AD' or 'BC') (str) from coin (BeautifulSoup) object'''
    description = coin_description(coin)
    try:
        match = re.search(r'\b(AD|BC)\b', description)
        return match.group(0)
    except:
        return None

def coin_year(coin):
    '''Returns a year (int) from coin (BeautifulSoup) object'''
    description = coin_description(coin)
    try:
        year_matches = re.findall(r'\b(AD|BC)\s*(\d{1,3})(?:/(\d{1,3})|-(\d' +
                                  r'{1,3}))?\b|\b(\d{1,3})(?:/(\d{1,3})|-(' +
                                  r'\d{1,3}))?\s*(AD|BC)\b', description)
    except:
        return None

    valid_years = []
    for match in year_matches:
        era1, start_year1, end_year1_slash, end_year1_dash, start_year2, end_year2_slash, end_year2_dash, era2 = match
        era = era1 or era2
        start_year = int(start_year1 or start_year2)
        end_year = int(end_year1_slash or end_year1_dash or end_year2_slash or 
                       end_year2_dash) if end_year1_slash or end_year1_dash or \
                        end_year2_slash or end_year2_dash else None

        year = start_year

        if year and -50 <= year <= 500:
            valid_years.append(year if era not in ['BC', 'B.C.'] else -year)

    return min(valid_years) if valid_years else None

def coin_txt(coin, title):
    '''Returns .txt url (str) from coin (BeautifulSoup) object'''
    base_url = 'https://www.wildwinds.com/coins/ric/'
    for level in [2, 1, 3, 4, 5]:
        try:
            for a in coin[level].find_all('a', href=True):
                filename = a['href']
                if '.txt' in filename:
                    return base_url + title.replace(' ', '_').lower() + '/' + filename
        except:
            continue
    return None

def coin_mass(coin):
    '''Returns mass (float, in g) from coin (BeautifulSoup) object'''
    try:
        description = coin_description(coin)
        match = re.search(r'(\d+((\.|\,|\-)\d+)?)\s?(?:g|gm|gr)\b', description)
        mass = float(match.group(1).replace(',', '.').replace('-', '.'))
        return mass if 0 < mass < 50 else None
    except:
        return None

def coin_diameter(coin):
    '''Returns diameter (float, in mm) from coin (BeautifulSoup) object'''
    try:
        description = coin_description(coin)
        match = re.search(r'(\d{1,2}(\.\d+)?)\s?(?:mm)', description)
        diameter = float(match.group(1))
        return diameter if 0 < diameter <= 50 else None
    except:
        return None

# Check for common inscriptions
''' ...such as "AVG" (Augustus, title of the emperor), "IMP" (Imperator 
(victorious general), received upon accession), "CAES" (Caesar, inherited name 
of the Julian family (Julius Caesar), used by later emperors to designate heir), 
"GERM" (Germanicus, a title honoring military victories in Germany), "COS" or 
"CONSVL" (Consul, a title linked to highest office in Senate, usually held by 
emperor), "PO" (Pontifex Maximus, highest priest, the head of state religion), 
"PP" (Pater Patriae, father of the country), "PF" (Pius Felix, reverent or 
dutiful), "SC" (Senatus Consultus), "TPP" (Tribunica Potestate, tribune of the 
people, each renewal indicated by numerals), "CENS" (Censor, a public office 
overseeing taxes, morality, the census and membership in various orders), 
"BRIT" (Britannicus).'''

def coin_inscriptions(coin):
    '''Returns recognized inscriptions (str:'EX1,EX2') from coin (BeautifulSoup) object'''
    inscriptions_list = ['AVG', 'IMP', 'CAES', 'GERM', 'COS', 'CONSVL', 'PP', 
                         'PO', 'PF', 'SC', 'CENS', 'TPP', 'TR', 'RESTITVT', 
                         'BRIT', 'AVGVSTVS', 'CAESAR', 'C', 'TRIB', 'POT', 'PON',
                         'MAX', 'PM', 'SPQR', 'S P Q R', 'S-C', 'TRP', 'PAX']
    try:
        description = coin_description(coin)
        inscriptions = {i for i in inscriptions_list if f' {i} ' in description or f' {i},' in description}
        return ','.join(sorted(list(inscriptions))) if inscriptions else None
    except:
        return None

def coins_from_soup(soup:BeautifulSoup):
    '''Returns a list of parsed coins as col:val dicts'''
    title = pull_title(soup)
    subtitle = pull_subtitle(soup)
    coins = []
    for coin in pull_coins(soup):
        if coin_description(coin):
            current_datetime = datetime.datetime.now()
            coins.append({
                'id':coin_id(),
                'name':title,
                'name_detail':subtitle,
                'catalog':coin_catalog(coin),
                'description':coin_description(coin),
                'metal':coin_metal(coin),
                'mass':coin_mass(coin),
                'diameter':coin_diameter(coin),
                'era':coin_era(coin),
                'year':coin_year(coin),
                'inscriptions':coin_inscriptions(coin),
                'txt':coin_txt(coin, title=title),
                'created':current_datetime,
                'modified':current_datetime
            })

    return coins if coins else None

def load_coins(coins:list[dict] | None, conn:psycopg2.extensions.connection, table:str, commit:bool=True):
    '''Loads a list of coins into a postgres table using SQL INSERT statements'''
    try:
        cur = conn.cursor()
        for coin in coins:
            columns = ', '.join(coin.keys())
            placeholders = ', '.join(f'%({col})s' for col in coin.keys())
            query = f'INSERT INTO {table} ({columns}) VALUES ({placeholders});'
            cur.execute(query, coin)
        if commit:
            conn.commit()            
    except psycopg2.Error as e:
        print('Load error:', e)
        conn.rollback()
    finally:
        cur.close()

def check_state(path:str):
    '''Returns last row of csv state file if it exists, else None'''
    if os.path.exists(path):
        with open(path) as state_file:
            reader = csv.reader(state_file)
            rows = [row for row in reader]
        state = rows[-1][0]
        if state == 'Scraping/Loading complete':
            print('Scraping already completed. Exiting.')
            exit()
        return state
    return None
        
def update_state(path:str, input:str):
    ''' Creates csv state file if one doesn't exist; appends it with input'''
    with open(path, 'a') as state:
        w = csv.writer(state)
        w.writerow([input])

def scrape_and_load(conn:psycopg2.extensions.connection, state_path:str | None, pages:list[str], table:str, delay:int=30):
    '''Composite function scrapes a page for coins and loads them into postgres table'''
    state = check_state(state_path)
    if state is not None:
        checkpoint = pages.index(state) + 1
    else:
        checkpoint = 0
    for page in pages[checkpoint:]:
        print(f'requesting {page} ({pages.index(page) + 1}/{len(pages)})')
        sleep(delay)
        soup = scrape_page(page)
        coins = coins_from_soup(soup)
        if coins:
            print(f'loading {len(coins)} coins into database {db_info["db_name"]} as {db_info["db_user"]}...')
            load_coins(coins, conn, table)
        update_state(state_path, page)
    message = 'Scraping/Loading complete'
    update_state(state_path, message)
    print(f'{message}: {len(pages)} pages')

def main():
    '''Scrapes, processes, and loads data from over 200 page requests, which 
    takes a couple hours due to required 30-second delay between requests)'''
    with connect_db(**db_info) as conn:
        create_table(conn, table_name, table_columns, column_dtypes)
    conn.close()
    print("Sourcing Roman Empire coin pages...")
    empire_pages = list(set(get_pages('https://www.wildwinds.com/coins/ric/i.html')))
    sleep(30)
    print("Sourcing Roman Republic coin pages...")
    republic_pages = list(set(get_pages('https://www.wildwinds.com/coins/rsc/i.html')))
    combined_pages = sorted(empire_pages + republic_pages)
    with connect_db(**db_info) as conn:
        scrape_and_load(conn, state_path, combined_pages, table_name)
    conn.close()

if __name__ == '__main__':
    main()