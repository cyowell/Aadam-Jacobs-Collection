# ... inside the update_data.py script ...

# 3. Append only the truly new items
with open(FILE_PATH, 'a', newline='', encoding='utf-8') as f:
    # UPDATED: Removed coverage and runtime to match your header
    fields = ['identifier', 'title', 'date', 'creator', 'venue', 'url', 'addeddate']
    writer = csv.DictWriter(f, fieldnames=fields)
    
    for item_id in new_items:
        try:
            item = ia.get_item(item_id)
            m = item.metadata
            added_dt = m.get('addeddate', 'N/A')
            
            if added_dt <= last_checkpoint:
                continue

            title = m.get('title', '')
            venue = m.get('venue', 'N/A')
            if venue == 'N/A' and ' at ' in title:
                venue = title.split(' at ', 1)[1].strip()

            # UPDATED: Only writing the 7 columns you kept
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