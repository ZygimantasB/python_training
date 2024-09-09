# https://www.youtube.com/watch?v=nWxwqxb0FCk&list=PLxhYs0VkONL9h5zHZXNntPqLC1JVg9pxX&index=148

import duckdb
import pandas as pd
import polars as pl

# print(duckdb.read_csv('../data/mydata.csv'))

result = duckdb.sql("SELECT * FROM read_csv_auto('../data/mydata.csv') WHERE age > 40")

df = pd.read_csv('../data/mydata.csv')

# print(result.fetchall())
# print(result.to_df())
# print(result.df())
# print(result.pl)
# print(result.fetchnumpy())

conn = duckdb.connect('somedb.ddb')

conn.sql("CREATE TABLE IF NOT EXISTS people as Select * FROM read_csv_auto('../data/mydata.csv')")

# print(conn.execute("SELECT * FROM people WHERE age > 40").fetchdf())
conn.register('people', df)

print(conn.execute("SELECT * FROM people WHERE age > 40").fetchdf())

conn.commit()
conn.close()

