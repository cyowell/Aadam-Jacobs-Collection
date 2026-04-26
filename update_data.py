import internetarchive as ia
import csv
import os
import re

# --- CONFIGURATION ---
FILE_PATH = 'data/aadam_refined_data.csv'
# The Internet Archive collection identifier for the Aadam Jacobs Collection
IA_COLLECTION = 'aadamjacobs'
# The BlueSky handle for the project account
BSKY_HANDLE = 'ajcproject.bsky.social'

def normalize_date(dt_str):
    """Normalize an IA addeddate string (e.g. '2025-10-09 00:22:50' or '2025-10-09T00:22:50Z')
    to a plain YYYY-MM-DD string for consistent comparison.
    Returns None if the string is not in a recognizable ISO format."""
    if not dt_str or dt_str in ('N/A', ''):
        return None
    # Strip time component if present (handles both space and T separators)
    date_part = re.split(r'[ T]', dt_str.strip())[0]
    # Only accept YYYY-MM-DD format (not legacy M/D/YY format from old CSV rows)
    if re.match(r'^\d{4}-\d{2}-\d{2}$', date_part):
        return date_part
    return None

# --- 1. Load existing IDs and find the most recent addeddate ---
existing_ids = set()
last_checkpoint = "1900-01-01"

if os.path.exists(FILE_PATH):
    with open(FILE_PATH, 'r', encoding='utf-8') as f:
        reader = list(csv.DictReader(f))
        for row in reader:
            existing_ids.add(row['identifier'])
            raw_date = row.get('addeddate', '')
            normalized = normalize_date(raw_date)
            if normalized and normalized > last_checkpoint:
                last_checkpoint = normalized

print(f"Loaded {len(existing_ids)} existing records. Last checkpoint: {last_checkpoint}")

# --- 2. Part A: Search the Internet Archive by collection ---
print(f"Checking Internet Archive collection '{IA_COLLECTION}' for items after: {last_checkpoint}")
query = f"collection:{IA_COLLECTION} AND addeddate:[{last_checkpoint} TO 9999-12-31]"
search = ia.search_items(
    query,
    fields=['identifier', 'addeddate'],
)
potential_ids = []
for result in search:
    item_id = result.get('identifier')
    if item_id:
        potential_ids.append(item_id)
print(f"Found {len(potential_ids)} candidate(s) from Internet Archive search.")

# --- 3. Part B: Check the BlueSky Feed for archive.org links ---
print(f"Cross-referencing BlueSky feed: {BSKY_HANDLE}")
try:
    from atproto import Client
    client = Client()
    response = client.app.bsky.feed.get_author_feed({'actor': BSKY_HANDLE, 'limit': 50})
    bsky_count = 0
    for feed_view in response.feed:
        text = feed_view.post.record.text
        # Extract IA identifiers from archive.org URLs posted in the feed
        # Matches patterns like: archive.org/details/ajc00123_some-band-2001-01-01
        found_via_url = re.findall(r'archive\.org/details/([\w.-]+)', text)
        for match in found_via_url:
            if match not in existing_ids and match not in potential_ids:
                potential_ids.append(match)
                bsky_count += 1
    print(f"Found {bsky_count} additional candidate(s) from BlueSky feed.")
except ImportError:
    print("BlueSky check skipped: 'atproto' package not installed.")
except Exception as e:
    print(f"BlueSky check skipped: {e}")

# --- 4. Deduplicate and filter out already-known IDs ---
new_ids = [i for i in dict.fromkeys(potential_ids) if i not in existing_ids]
print(f"\n{len(new_ids)} new item(s) to add.")

if not new_ids:
    print("Everything is up to date!")
    exit(0)

# --- 5. Fetch full metadata and append to CSV ---
added_count = 0
with open(FILE_PATH, 'a', newline='', encoding='utf-8') as f:
    fields = ['identifier', 'title', 'date', 'creator', 'venue', 'url', 'addeddate']
    writer = csv.DictWriter(f, fieldnames=fields)

    for item_id in new_ids:
        try:
            item = ia.get_item(item_id)
            if not item.exists:
                print(f"Skipped (not found on IA): {item_id}")
                continue

            m = item.metadata
            raw_added = m.get('addeddate', 'N/A')
            added_date_normalized = normalize_date(raw_added)

            # Guard: skip if somehow still before checkpoint (shouldn't happen, but be safe)
            if added_date_normalized != 'N/A' and added_date_normalized < last_checkpoint:
                print(f"Skipped (before checkpoint): {item_id} ({added_date_normalized})")
                continue

            title = m.get('title', 'N/A')
            venue = m.get('venue', 'N/A')
            # Fallback: extract venue from title if not in metadata (e.g. "Band Live at Venue 2001-01-01")
            if venue == 'N/A' and ' at ' in title:
                venue = title.split(' at ', 1)[1].strip()

            writer.writerow({
                'identifier': item_id,
                'title': title,
                'date': m.get('date', 'N/A'),
                'creator': m.get('creator', 'N/A'),
                'venue': venue,
                'url': f"https://archive.org/details/{item_id}",
                'addeddate': added_date_normalized,
            })
            print(f"  Added: {item_id} ({added_date_normalized})")
            existing_ids.add(item_id)
            added_count += 1

        except Exception as e:
            print(f"  Error on {item_id}: {e}")

print(f"\nDone. {added_count} new record(s) written to {FILE_PATH}.")