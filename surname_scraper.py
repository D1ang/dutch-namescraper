"""Scraping module for Dutch surnames from the CBG Familienamen database."""

from ratelimit import sleep_and_retry, limits
from bs4 import BeautifulSoup
from pathlib import Path
import urllib3

import string
import requests
import json

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


@sleep_and_retry
@limits(calls=1, period=2)  # one call per two seconds
def parse_table(page: BeautifulSoup) -> list[list[str]]:
    """Parse the HTML table from a BeautifulSoup page object."""
    # Find the hitlist table containing the surnames
    table = page.find('table', {'id': 'hitlist'})
    if not table:
        return []
    
    rows = table.find_all('tr')
    results = []
    
    # Skip the header row (first row)
    for row in rows[1:]:
        cells = row.find_all('td')
        if len(cells) >= 3:  # We need at least 3 columns for surname, count, normalized
            surname = cells[0].text.strip()
            count = cells[1].text.strip()
            normalized = cells[2].text.strip()
            
            # Only include valid entries (skip empty surnames)
            if surname:
                results.append([surname, count, normalized])
    
    return results


def get_page_urls(base_url: str, letter: str) -> list[str]:
    """Get all page URLs for a given letter."""
    try:
        response = requests.get(f"{base_url}?operator=bw&naam={letter}", verify=False)
        soup = BeautifulSoup(response.text, features='html.parser')
        
        page_urls = []
        
        # Look for pagination links in the format [1-50] [51-100] etc.
        links = soup.find_all('a')
        for link in links:
            href = link.get('href', '')
            text = link.text.strip()
            
            # Look for pagination patterns like "51-100", "101-150", etc.
            if ('offset=' in href and 'naam=' in href and 'operator=bw' in href and
                '-' in text and text.replace('-', '').replace(' ', '').isdigit()):
                
                if href.startswith('/nfb/'):
                    full_url = f"https://www.cbgfamilienamen.nl{href}"
                elif href.startswith('lijst_namen.php'):
                    full_url = f"https://www.cbgfamilienamen.nl/nfb/{href}"
                else:
                    full_url = href
                
                page_urls.append(full_url)
        
        # Add the first page (no offset)
        first_page = f"{base_url}?operator=bw&naam={letter}"
        if first_page not in page_urls:
            page_urls.insert(0, first_page)
        
        # Sort by offset to get pages in order
        def extract_offset(url):
            if 'offset=' in url:
                try:
                    return int(url.split('offset=')[1].split('&')[0])
                except (ValueError, IndexError):
                    return 0
            return 0
        
        page_urls.sort(key=extract_offset)
        return page_urls
        
    except Exception as e:
        print(f"Error getting page URLs: {e}")
        return [f"{base_url}?operator=bw&naam={letter}"]


print('Starting scraping Dutch surnames...')

base_url = "https://www.cbgfamilienamen.nl/nfb/lijst_namen.php"
total_names_collected = 0

for name, letter in enumerate(string.ascii_uppercase, 1):  # Using uppercase as the site expects
    print(f"Processing letter '{letter}' ({name}/26)...")

    # Open file for this letter immediately
    filename = f'surnames_{letter.lower()}.json'
    names = []
    
    try:
        # Get all page URLs for this letter
        page_urls = get_page_urls(base_url, letter)
        print(f"   Found {len(page_urls)} pages for letter '{letter}'")
        
        for page_num, url in enumerate(page_urls, 1):
            print(f"   Fetching page {page_num}/{len(page_urls)} for letter '{letter}'...", end=' ', flush=True)
            
            try:
                response = requests.get(url, verify=False)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, features='html.parser')
                results = parse_table(soup)
                
                if results:
                    names.extend(results)
                    total_names_collected += len(results)
                    print(f'Found {len(results)} surnames (total so far: {total_names_collected})')
                    
                    # Write to file immediately after each page
                    with Path(filename).open('w') as w:
                        w.write(json.dumps(names, indent=2))
                    print(f'   Updated {filename} with {len(names)} surnames')
                else:
                    print('No surnames found')
                    
            except Exception as e:
                print(f'Error fetching page: {e}')
                continue
                
    except Exception as e:
        print(f'Error processing letter {letter}: {e}')
        continue

    print(f"   Completed letter '{letter}': {len(names)} surnames saved to {filename}")
    print(f'   Total surnames collected so far: {total_names_collected}\n')
