import sqlite3
conn = sqlite3.connect("process_data.db")
cursor = conn.cursor()

cursor.execute("PRAGMA table_info(process_data)")
columns = cursor.fetchall()
for col in columns:
    print(col)

conn.close()
