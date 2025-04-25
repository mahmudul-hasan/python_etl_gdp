# This is a Web Scraping ETL Project in Python
This project uses a python script to perform web scraping and ETL process. 

**Extract:** The script extracts countries' GDP (in mission USD) data using web scraping. 

**Transform:** The script then transforms that GDP in billion USD and rounds to the two decimal points. 

**Load:** After that, the script loads the data into a CSV file as well as into a database. 

After the ETL process, the script runs a query into the database. The entire ETL process is logged and timestamped through the log_progress() function. 

## Libraries Used: 
- **BeautifulSoup** - for web scraping
- **Pandas** - for transforming the scraped data into a DataFrame and adding more processed data columns.
- **Numpy** - for the data processing and conversion.
- **SQLite3** - for loading the data into a database.  
