import sqlite3

conn = sqlite3.connect(r'd:\misaka10843\Documents\GitHub\pixiv-agent\data\agent.db')
conn.execute("UPDATE artworks SET fetched_at=datetime('now') WHERE pixiv_id IN (SELECT pixiv_id FROM images WHERE downloaded=1)")
conn.commit()
print("Updated artworks count:", conn.total_changes)
