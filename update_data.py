import internetarchive as ia
from atproto import Client
import csv
import os
import re

# --- CONFIGURATION ---
FILE_PATH = 'data/aadam_refined_data.csv'
IA_UPLOADER = '@aadam_jacobs_collection'
BSKY_HANDLE = 'ajcproject.mas.to.ap.brid.gy'

# 1. Load existing IDs to avoid duplicates
existing_ids = set()
last_checkpoint = "1900-01-01"

if os.path.exists(FILE_PATH):
    with open(FILE_PATH, 'r', encoding='utf-8') as f:
        reader = list(csv.DictReader(f))
        for row in reader:
            existing_ids.add(row['identifier'])
            if row.get('addeddate') and row['addeddate'] > last_checkpoint:
                last_checkpoint = row['addeddate']

# 2. Part A: Check the Internet Archive (Elegant Search)
print(f"Checking IA for items after: {last_checkpoint}")
query = f"uploader:{IA_UPLOADER} AND addeddate:[{last_checkpoint} TO 9999-12-31] sort:addeddate"
search = ia.search_items(query)
potential_ids = [result['identifier'] for result in search]

# 3. Part B: Check the BlueSky Feed (Cross-Reference)
print(f"Cross-referencing BlueSky feed: {BSKY_HANDLE}")
try:
    client = Client()
    # Fetch latest posts from the profile
    response = client.app.bsky.feed.get_author_feed({'actor': BSKY_HANDLE, 'limit': 30})
    for feed_view in response.feed:
        text = feed_view.post.record.text
        # Look for Archive IDs in the post text (patterns like ajcXXXX or 01XX)
        found = re.findall(r'[\w-]+(?=\s|/|$)', text) 
        for match in found:
            # Simple filter to find IDs likely to be IA identifiers
            if match.startswith(('ajc', '01', '02')) and match not in existing_ids:
                potential_ids.append(match)
except Exception as e:
    print(f"BlueSky check skipped: {e}")

# 4. Remove duplicates from our final list of new IDs
new_ids = [i for i in dict.fromkeys(potential_ids) if i not in existing_ids]

if not new_ids:
    print("Everything is up to date!")
    exit(0)

# 5. Fetch and Append Metadata
with open(FILE_PATH, 'a', newline='', encoding='utf-8') as f:
    fields = ['identifier', 'title', 'date', 'creator', 'venue', 'url', 'addeddate']
    writer = csv.DictWriter(f, fieldnames=fields)
    
    for item_id in new_ids:
        try:
            item = ia.get_item(item_id)
            if not item.exists: continue
            
            m = item.metadata
            added_dt = m.get('addeddate', 'N/A')
            
            # Double check date to prevent adding back the checkpoint item
            if added_dt != 'N/A' and added_dt <= last_checkpoint:
                continue

            title = m.get('title', '')
            venue = m.get('venue', 'N/A')
            if venue == 'N/A' and ' at ' in title:
                venue = title.split(' at ', 1)[1].strip()

            writer.writerow({
                'identifier': item_id,
                'title': title,
                'date': m.get('date', 'N/A'),
                'creator': m.get('creator', 'N/A'),
                'venue': venue,
                'url': f"https://archive.org/details/{item_id}",
                'addeddate': added_dt
            })
            print(f"Added: {item_id}")
            existing_ids.add(item_id) # Prevent adding same ID twice in one run
        except Exception as e:
            print(f"Error on {item_id}: {e}")