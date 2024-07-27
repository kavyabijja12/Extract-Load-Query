import pandas

class Query:
    def __init__(self) -> None:
        pass

    def query_data(self,query_statement,sql_connection):
        query_output = pandas.read_sql(query_statement, sql_connection)
        print("Query Output: \n", query_output)
        sql_connection.close()