import sqlite3
import os

db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "agent.db")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute("SELECT count(*), count(series_json), count(CASE WHEN series_json != '{}' AND series_json IS NOT NULL THEN 1 END) FROM artworks;")
res = cursor.fetchone()
print(f"Total artworks: {res[0]}, With series_json col: {res[1]}, Valid series_json: {res[2]}")

# 查出有 series 的一条来看看
cursor.execute("SELECT pixiv_id, series_json FROM artworks WHERE series_json != '{}' AND series_json IS NOT NULL LIMIT 1;")
row = cursor.fetchone()
if row:
    print(f"Example: ID={row[0]}, Series={row[1]}")
else:
    print("No valid series found.")
