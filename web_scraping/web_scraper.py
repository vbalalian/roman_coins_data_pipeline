#!/usr/bin/env python
# coding: utf-8

# Import libraries
import numpy as np
import pandas as pd
from time import sleep
import requests
from bs4 import BeautifulSoup
import re

# Function to pull html from test pages
def scrape(url_roots: list[str], delay: int = 30):
    combined_pages_html = []
    max_length = 0
    for root in url_roots:
        url = root + 'i.html'
        message = f'requesting {url} ({url_roots.index(root) + 1}/{len(url_roots)})'
        max_length = max(max_length, len(message))
        print(f'\r{message.ljust(max_length)}', end=' ', flush=True)
        if root != url_roots[0]:
            sleep(delay)
        page_html = requests.get(url)
        combined_pages_html.append(page_html)
    # After the loop, print a final message that clears the last line
    print(f'\rscraping complete: {url_roots.index(root) + 1}/{len(url_roots)}'.ljust(max_length))
    return combined_pages_html

# Create helper functions for parsing data fields
# Function for parsing names of coin subjects
def pull_title(soup):
    raw_title = soup.find('title')
    if raw_title:
        title_text = raw_title.text
        sep_index = title_text.find(',')
        if sep_index == -1:
            sep_index = title_text.find('-')
        return title_text[:sep_index].strip() if sep_index != -1 else title_text.strip()
    return None

def pull_subtitle(soup):
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

# Function to pull raw coin data
def pull_coins(soup):
    coins = [coin.contents for coin in soup.find_all('tr') if len(coin) >2 and 'bgcolor' in str(coin)]
    return coins

# Function to pull coin ids
def coin_id(coin):
    try:
        id_html = coin[0]
        id = id_html.get_text()
        return id if id else None
    except IndexError:
        return None

# Function to pull coin descriptions
def coin_description(coin):
    try:
        for i in range(1, 4):
            chunk = coin[i]
            desc = chunk.get_text().replace('\r\n', '')
            if len(desc) >= 50:
                return desc
    except IndexError:
        return None

# Function to identify coin metal
def coin_metal(coin):
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

# Function to pull coin era (i.e. 'AD' or 'BC') 
def coin_era(coin):
    description = coin_description(coin)
    try:
        match = re.search(r'\b(AD|BC)\b', description)
        return match.group(0)
    except:
        return None

# Function to pull first year in the coin description
def coin_year(coin):
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

        if year:
            valid_years.append(year if era not in ['BC', 'B.C.'] else -year)

    return min(valid_years) if valid_years else None

# Function to pull .txt urls
def coin_txt(coin, title):
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

# Function to pull coin mass (in grams)
def coin_mass(coin):
    try:
        description = coin_description(coin)
        match = re.search(r'(\d+((\.|\,|\-)\d+)?)\s?(?:g|gm|gr)\b', description)
        return float(match.group(1).replace(',', '.').replace('-', '.'))
    except:
        return None

# Function to pull coin diameter (in mm)
def coin_diameter(coin):
    try:
        description = coin_description(coin)
        match = re.search(r'(\d+(\.\d+)?)\s?(?:mm)', description)
        return float(match.group(1))
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

# Function to pull recognized inscriptions
def coin_inscriptions(coin):
    description = coin_description(coin)
    inscriptions_list = ['AVG', 'IMP', 'CAES', 'GERM', 'COS', 'CONSVL', 'PP', 'PO', 'PF',
                         'SC', 'CENS', 'TPP', 'TR', 'RESTITVT', 'BRIT', 'AVGVSTVS', 'CAESAR',
                         'C', 'TRIB POT', 'PON MAX', 'PM']
    found_inscriptions = [i for i in inscriptions_list if f' {i} ' in coin]
    unique_inscriptions = list(set(found_inscriptions))
    return unique_inscriptions if unique_inscriptions else None

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
        txt.append(coin_txt(coin, title=title))
    return pd.DataFrame({'ruler':title, 'ruler_detail':subtitle, 'id':id, 'description':description, 'metal':metal, 'mass':mass, \
                        'diameter':diameter, 'era':era, 'year':year, 'inscriptions':inscriptions, 'txt':txt})

# Function to combine multiple coin Dataframes
def combine_coin_dfs(soups):
    dfs = [coin_df(soup) for soup in soups]
    filtered_dfs = [df for df in dfs if not df.empty and not df.isna().all().all()]
    return pd.concat(filtered_dfs, ignore_index=True) 

# Pull html from all source pages
''' (pulling from over 200 pages, which takes a couple hours with the 30 second 
delay between requests)'''
def main():
    # Scrape link directory
    with requests.get('https://www.wildwinds.com/coins/ric/i.html') as raw:
        soup = BeautifulSoup(raw.content, 'lxml')

    # Parse html data for a clean list of ruler names
    options = soup.find_all('option')
    emperors_raw = [i.contents for i in options if i.attrs['value'] != ''][:-6]
    emperors = []
    for line in emperors_raw:
        for text in line:
            emperors.append(text.strip())

    # Generate list of usable link roots for each Emperor's coin page
    # wildwinds.com/robots.txt requires a 30-second delay between requests
    linkroots = ['https://www.wildwinds.com/coins/ric/' + i.attrs['value'][:-6] for i in options if i.attrs['value'] != ''][:-6]

    all_pages = scrape(linkroots)

    # Parse html of each page using BeautifulSoup
    all_soups = [BeautifulSoup(page.content, 'lxml') for page in all_pages]

    # Combine it all into a single Dataframe
    roman_coins_raw = combine_coin_dfs(all_soups)

    # Remove duplicate coins
    roman_coins = roman_coins_raw.drop_duplicates(subset=['id'], keep='first').copy()

    # Map average year value of each ruler
    ruler_avg_year = {
        ruler:round(roman_coins[roman_coins['ruler'] == ruler]['year'].mean(), 1) 
        for ruler in roman_coins['ruler'].unique().tolist()
        }

    # Fill missing year values with average year value of matching ruler
    roman_coins.loc[roman_coins['year'].isna(), 'year'] = roman_coins['ruler'].map(ruler_avg_year)

    # Drop remaining NA year values
    roman_coins.dropna(subset='year', inplace=True)
    roman_coins['year'] = roman_coins['year'].astype(int)

    # Map years to eras
    BC_coins = roman_coins['year'] < 0
    AD_coins = roman_coins['year'] > 0

    # Fix era values to match year sign
    roman_coins.loc[BC_coins, 'era'] = 'BC'
    roman_coins.loc[AD_coins, 'era'] = 'AD'

    # Drop entries with missing metal values
    roman_coins.dropna(subset=['metal'], inplace=True)

    # Remove coins with outlier year values
    outlier_coins = (roman_coins['year'] < -50) | (roman_coins['year'] > 500)
    roman_coins = roman_coins[~outlier_coins]

    # Remove entries still missing id value
    roman_coins.dropna(subset=['id'], inplace=True)

    # Consolidate missing value types
    roman_coins.replace([np.inf, -np.inf, np.nan], None, inplace=True)

    # Export roman_coins DataFrame as csv
    roman_coins.to_csv('web_scraping/roman_coins.csv', index=False)

if __name__ == '__main__':
    main()