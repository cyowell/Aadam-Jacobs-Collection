import internetarchive as ia
import csv
import os

FILE_PATH = 'data/aadam_refined_data.csv'
COLLECTION = 'aadamjacobs'

# 1. Load existing identifiers to avoid duplicates
existing_ids = set()
if os.path.exists(FILE_PATH):
    with open(FILE_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            existing_ids.add(row['identifier'])

# 2. Search Internet Archive for current list
print("Checking for new uploads...")
search = ia.search_items(f'collection:{COLLECTION}')
new_ids = [result['identifier'] for result in search if result['identifier'] not in existing_ids]

if not new_ids:
    print("No new items found.")
    exit(0)

print(f"Found {len(new_ids)} new items. Fetching metadata...")

# 3. Append new items to the CSV
with open(FILE_PATH, 'a', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['identifier', 'title', 'date', 'creator', 'venue', 'coverage', 'runtime', 'url'])
    
    for item_id in new_ids:
        try:
            item = ia.get_item(item_id)
            m = item.metadata
            title = m.get('title', '')
            
            # Extract venue from title logic
            venue = m.get('venue', 'N/A')
            if venue == 'N/A' and ' at ' in title:
                venue = title.split(' at ', 1)[1].strip()

            writer.writerow({
                'identifier': item_id,
                'title': title,
                'date': m.get('date', 'N/A'),
                'creator': m.get('creator', 'N/A'),
                'venue': venue,
                'coverage': m.get('coverage', 'N/A'),
                'runtime': m.get('runtime', 'N/A'),
                'url': f"https://archive.org/details/{item_id}"
            })
            print(f"Added: {item_id}")
        except Exception as e:
            print(f"Error skipping {item_id}: {e}")