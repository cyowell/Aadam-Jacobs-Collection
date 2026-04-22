import internetarchive as ia
import csv
import os

# --- CONFIGURATION ---
FILE_PATH = 'data/aadam_refined_data.csv'
UPLOADER = '@aadam_jacobs_collection'

# 1. Determine the last upload timestamp we have
last_checkpoint = "1900-01-01" 
if os.path.exists(FILE_PATH):
    with open(FILE_PATH, 'r', encoding='utf-8') as f:
        reader = list(csv.DictReader(f))
        if reader:
            # Look for the most recent 'addeddate' in your file
            dates = [row['addeddate'] for row in reader if row.get('addeddate') and row['addeddate'] != 'N/A']
            if dates:
                last_checkpoint = max(dates)

print(f"Searching for uploads after: {last_checkpoint}")

# 2. Elegant Search
query = f"uploader:{UPLOADER} AND addeddate:[{last_checkpoint} TO 9999-12-31]"
# Change 'sort_by' to 'sort' and provide the sort string correctly
search = ia.search_items(query, sort=['addeddate'])

new_items = []
for result in search:
    new_items.append(result['identifier'])

if not new_items:
    print("Everything is up to date!")
    exit(0)

# 3. Append only the truly new items
with open(FILE_PATH, 'a', newline='', encoding='utf-8') as f:
    # Match your 7-column header exactly
    fields = ['identifier', 'title', 'date', 'creator', 'venue', 'url', 'addeddate']
    writer = csv.DictWriter(f, fieldnames=fields)
    
    for item_id in new_items:
        try:
            item = ia.get_item(item_id)
            m = item.metadata
            added_dt = m.get('addeddate', 'N/A')
            
            # Skip if we already have this exact timestamp
            if added_dt <= last_checkpoint:
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
        except Exception as e:
            print(f"Error skipping {item_id}: {e}")
