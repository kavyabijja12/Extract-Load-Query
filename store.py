import sqlite3

class Store:
    def __init__(self) -> None:
        self.db_name = 'SQliteDatabase.db'

    def store_to_db(self,df,table_name):
        conn = sqlite3.connect(self.db_name)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        return conn
