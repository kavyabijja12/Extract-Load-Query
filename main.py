from scrape import ScrapeData
from store import Store
from query import Query

table_name = "SqlTable"
# Extract data
cl = ScrapeData()
dataframe = cl.get_data()

# Load
cl= Store()
sql_conn = cl.store_to_db(dataframe,table_name)

# Query
cl = Query()
query_statement = f"select * from {table_name}"
cl.query_data(query_statement,sql_conn)

