# Code for ETL operations on Country-GDP data

# Importing the required libraries.
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

def extract(url, table_attribs):
    # Get the page from the URL
    page = requests.get(url).text
    # Create the dataframe with the table_attribs as the columns
    df = pd.DataFrame(columns=table_attribs)
    # Create the BeautifulSoup instance
    data = BeautifulSoup(page, "html.parser")
    # Find all the table bodies. There are 7 tables it looks like
    tables = data.find_all("tbody")
    # Find all the table rows from the 3rd table, which is containing all the countries and their GDPs, according to the webpage.
    rows = tables[2].find_all("tr")
    # Now for every row in rows, find all the table data td
    for row in rows:
        cols = row.find_all("td")
        if len(cols) != 0:
            country_col = cols[0]
            gdp_col = cols[2]
            if country_col.find("a") is not None and "—" not in gdp_col:
                country = country_col.a.contents[0]
                gdp = gdp_col.contents[0]
                data_dict = {
                    "Country": country,
                    "GDP_USD_millions": gdp
                }
                df_temp = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df_temp], ignore_index=True)
    return df

def transform(df):
    gdp_list = df["GDP_USD_millions"].tolist()
    gdp_list = [float("".join(gdp.split(","))) for gdp in gdp_list]
    # Now transform all the values by dividing by 1000 and round to the 2 decimal points
    gdp_list = [np.round(gdp/1000, 2) for gdp in gdp_list]
    df["GDP_USD_millions"] = gdp_list
    df = df.rename(columns={"GDP_USD_millions": "GDP_USD_billions"})
    return df

def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists="replace", index=False)

def run_query(query_statement, sql_connection):
    print(query_statement)
    output = pd.read_sql(query_statement, sql_connection)
    print(output)

def log_progress(message):
    time_format = "%Y-%h-%d-%H-%M-%S"
    now = datetime.now()
    timestamp = now.strftime(time_format)
    with open("./etl_project_log.txt", "a") as file:
        file.write(timestamp+" : "+message+"\n")


def run_etl_process():
    log_progress("Preliminaries complete. Initiating ETL process")
    df = extract(url, table_attribs)
    log_progress("Data extraction complete. Initiating transformation process")
    df = transform(df)
    log_progress("Data transformation complete. Initiating loading process")
    load_to_csv(df, csv_path)
    log_progress("Data saved to CSV file")
    sql_connection = sqlite3.connect(db_name)
    log_progress("SQL connection initiated")
    load_to_db(df, sql_connection, table_name)
    log_progress("Data loaded to database as table. Running the query")
    query = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
    run_query(query, sql_connection)
    log_progress("Process complete.")
    sql_connection.close()

url = "https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/list_of_countries_by_GDP_%28nominal%29"
table_attribs = ["Country", "GDP_USD_millions"]
db_name = "World_Economies.db"
table_name = "Countries_by_GDP"
csv_path = "./Countries_by_GDP.csv"

run_etl_process()

