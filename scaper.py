"""Scraping module for Dutch first names from the Meertens Institute."""

from ratelimit import sleep_and_retry, limits
from bs4 import BeautifulSoup
from pathlib import Path

import more_itertools
import string
import requests
import json


@sleep_and_retry
@limits(calls=1, period=2)  # one call per two seconds
def parse_table(page: BeautifulSoup) -> list[list[str]]:
    """Parse the HTML table from a BeautifulSoup page object."""
    return list(more_itertools.chunked([item.text for item in page.find_all('td')][3:], 3))


print('Starting scraping Dutch first names...')

total_names_collected = 0

for name, letter in enumerate(string.ascii_lowercase, 1):
    print(f"Processing letter '{letter.upper()}' ({name}/26)...")

    # Open file for this letter immediately
    filename = f'{letter}.json'
    names = []
    count = 1
    results = True

    while results:
        print(f"   Fetching page {count} for letter '{letter.upper()}'...", end=' ', flush=True)

        try:
            response = requests.get(f'https://www.meertens.knaw.nl/nvb/naam/pagina{count}/begintmet/{letter}')
            index = BeautifulSoup(response.text, features='html.parser')
            results = parse_table(index)

            if results:
                names.extend(results)
                total_names_collected += len(results)
                print(f'Found {len(results)} names (total so far: {total_names_collected})')

                # Write to file immediately after each page
                with Path(filename).open('w') as w:
                    w.write(json.dumps(names, indent=2))
                print(f'   Updated {filename} with {len(names)} names')
            else:
                print('No names found')
        except Exception as e:
            print(f'Error: {e}')
            break

        count += 1

    print(f"   Completed letter '{letter.upper()}': {len(names)} names saved to {filename}")
    print(f'   Total names collected so far: {total_names_collected}\n')

print('Combining all names and removing duplicates...')
all_names = set()
for letter in string.ascii_lowercase:
    filename = f'{letter}.json'
    if Path(filename).exists():
        print(f'   Reading {filename}...')
        with Path(filename).open() as f:
            data = [tuple(x) for x in json.loads(f.read())]
            all_names.update(data)
        Path(filename).unlink()
        print(f'   Deleted temporary file {filename}')

final_filename = 'first_names.json'
print(f"\nSaving {len(all_names)} unique names to '{final_filename}'...")
with Path(final_filename).open('w') as w:
    w.write(json.dumps(sorted(all_names), indent=2))

print(f"Done! All Dutch first names have been scraped and saved to '{final_filename}'.")
print(f'Final count: {len(all_names)} unique names')
