import duckdb, os

with open('sql/schema.sql','r',encoding='utf-8') as f:
    schema = f.read()

os.makedirs('data', exist_ok=True)
con = duckdb.connect('data/quant.duckdb')
con.execute(schema)
print('[INIT] DuckDB schema created at data/quant.duckdb')