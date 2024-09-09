import duckdb
import pandas as pd

import sqlite3

# conn = sqlite3.connect('somedb.db')
conn = duckdb.connect('somedb1.db')

cur = conn.cursor()

cur.execute("CREATE TABLE IF NOT EXISTS person (id INT, name TEXT)")
cur.execute("INSERT INTO person (id, name) VALUES (1, 'Mike');")

conn.commit()
cur.close()
conn.close()

duckdb.sql()
