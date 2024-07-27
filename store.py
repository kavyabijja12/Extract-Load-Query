import sqlite3

class Store:
    def __init__(self) -> None:
        self.db_name = 'Movies.db'
        self.table_name = 'Top_50'

    def store_to_db(self,df):
        conn = sqlite3.connect(self.db_name)
        df.to_sql(self.table_name, conn, if_exists='replace', index=False)
        conn.close()
