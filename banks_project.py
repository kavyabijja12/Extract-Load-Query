import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime 
import pandas as pd
import sqlite3

# For Logs 
log_file = "code_log.txt" 
table_name = "Largest_banks"
Sqlitedb = "Banks.db"
target_file = "Largest_banks_data.csv" 
query = f"select * from {table_name}"

# Importing the required libraries

def extract():
    url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'

    df = pd.DataFrame()
    count = 0
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    tables = data.find_all('table')
    rows = tables[0].find_all('tr')

    for row in rows:
        if count<10:
            col = row.find_all('td')
            if len(col)!=0:
                data_dict = {
                            "Name": col[1].find_all('a')[1].string,
                            "MC_USD_Billion": col[2].contents[0].strip()}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)
                count+=1
        else:
            break

    return df


def transform(df):
    # Convert the 'Number' column to numeric, setting errors='coerce' to handle non-numeric values
    df['MC_USD_Billion'] = pd.to_numeric(df['MC_USD_Billion'], errors='coerce')
    
    # Calculate the conversions and round the results
    df['MC_GBP_Billion'] = round(df['MC_USD_Billion'] * 0.8, 2)
    df['MC_EUR_Billion'] = round(df['MC_USD_Billion'] * 0.93, 2)
    df['MC_INR_Billion'] = round(df['MC_USD_Billion'] * 82.95, 2)

    return df

def load_to_csv(df):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(target_file) 


def load_to_db(df):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    df = pd.read_csv(target_file)
    conn = sqlite3.connect(Sqlitedb)
    df.to_sql(table_name, conn, if_exists = 'replace', index =False)
    return conn

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    query_statement = f"SELECT * FROM {table_name}"
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_statement)
    print(query_output)
    print('Table is ready')
    sql_connection.close()

def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 


# Extract, Transform, Load, log
# Log the initialization of the ETL process 
log_progress("Job Started") 
 
# Log the beginning of the Extraction process 
log_progress("Extract phase Started") 
extracted_data = extract() 
print(extracted_data)
 
# Log the completion of the Extraction process 
log_progress("Extract phase Ended") 
 
# Log the beginning of the Transformation process 
log_progress("Transform phase Started") 
transformed_data = transform(extracted_data) 
print("Transformed Data") 
print(transformed_data) 
 
# Log the completion of the Transformation process 
log_progress("Transform phase Ended") 
 
# Log the beginning of the Loading process 
log_progress("Load to csv phase Started") 
load_to_csv(transformed_data) 
 
# Log the completion of the Loading process 
log_progress("Load to csv phase Ended") 

# Log the beginning of the Loading to db 
log_progress("Load to db phase Started") 
sql_connection = load_to_db(transformed_data)
 
# Log the completion of the Loading to db 
log_progress("Load to db phase Ended") 

log_progress("Query from db phase Started") 
run_query(query,sql_connection)
 
# Log the completion of the Loading to db 
log_progress("Query from db phase Ended")
 
# Log the completion of the ETL process 
log_progress("Job Ended") 
