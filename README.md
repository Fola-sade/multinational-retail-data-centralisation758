# MULTINATIONAL RETAIL DATA CENTRALISATION

In this project, I'll  extract transform, load and analyse large datasets from multiple data sources. By utilising python programming language and its libraries, Use Case: creating a system that will centralize the sales data of Company A stored in several data sources.

- Developed a system that extracts retail sales data from five different data sources; PDF documents; an AWS RDS database; RESTful API, JSON and CSV files.
- Created a Python class which cleans and transforms over 120k rows of data before being loaded into a Postgres database.
- Developed a star-schema database, joining 5 dimension tables to make the data easily queryable allowing for sub-millisecond data analysis
- Used complex SQL queries to derive insights and to help reduce costs by 15%
- Queried the data using SQL to extract insights from the data; such as velocity of sales; yearly revenue and regions with the most sales. 

Key Technologies used: Postgres, Python(Pandas), boto3, AWS(s3), rest-API, csv.

## Milestone 1: 

A new Github repo was created for this projects

## Milestone 2:

- The first step is to set up a new database within pgadmin4,called sales_data. pgAdmin is the most popular and feature rich Open Source administration and development platform for PostgreSQL, the most advanced Open Source database in the world.I downloaded Postgres version 17 and logged into it in pgadmin4.  The sales_data database created in pgadmin will store the extracted tables from multitple data sources. After collecting all the tables, i'll then create a STAR based schema in a later part of this project. 

    -- Challenge faced: Path Conflicts between different versions of applications due to not including the necessary filepath on the Environment Variable, paticularly in the PATH variable. My operating system could not locate and execute the application from any directory without needing to specify its full path every time. 

- Three files were created to initialize the 3 classes (DataExtractor, DatabaseConnector and DataCleaning ) that will be used for the project. The files are data_extraction.py, database_utils.py and data_cleaning.py respectively. 


## data_extraction.py (DataExtractor)

This script will contain several methods that will be used to extract data from different sources (PDF documents, an AWS RDS database; RESTful API, JSON and CSV files).

- Relational Database Service (RDS): Making use of an instance of the DatabaseConnector, the method [read_rds_table] uses an engine to extract a specific table from the RDS.
- PDF: Using tabula to extract the information from the pdf url, and concatenating all pages, a dataframe is created. The method is named [retrieve_pdf_data]
- RESTful API: Making use of requests, the API is accessed and the information is extracted and managed. The methods for this use are [list_number_of_stores], [retrieve_stores_data] and [extract_date_details]
- s3 CSV: Making use of the boto3 package, it extracts a CSV file that is read with pandas. [extract_from_s3].

## database_utils.py

This script will contain methods to read credential files, create engines, navigate through the information in the databases and upload the specific tables on a local database(sales_data).


## data_cleaning.py

This script will contain several methods to clean the data that is being collected from multiple data sources, before uploading them into the local database.

## Overall.py

This extra script was created to integrate the classes.

Hence, once this is run, the postgres database can be accessed and all the information can be accessed in the sales_data database.

### Tasks processes

- AWS RDS database: The historical data of users is currently stored in an AWS database in the cloud. The data is extracted, cleaned and strored in the table named "dim_users" on local postgreSQL database.
- AWS S3 bucket: The users card details are stored in a PDF document in an AWS S3 bucket.We use the tabula-py Python package, imported with tabula to extract all pages from the pdf document Then return a DataFrame of the extracted data. The data is cleaned and stored in "dim_card_details" table on local database.
- The restful-API:The store data can be retrieved through the use of an API.The API has two GET methods. One will return the number of stores in the business and the other to retrieve a store given a store number.To connect to the API you will need to include the API key to connect to the API in the method header. The ".json" response has to be converted into the pandas dataframe. The data is cleaned and stored in "dim_store_details" table on local database.
- AWS s3 bucket: The information for each product the company currently sells is stored in CSV format in an S3 bucket on AWS. We use boto3 package to download and extract the information returning a pandas DataFrame.The data is cleaned and stored in "dim_products" table on local database.
- AWS RDS database: This table which acts as the single source of truth for all orders the company has made in the past is stored in a database on AWS RDS.The data is extracted, cleaned and strored in the table named "dim_orders" on local postgreSQL database.
-AWS s3 bucket: The final source of data is a JSON file containing the details of when each sale happened, as well as related attributes.The file is currently stored on S3.The data is cleaned and stored in "dim_date_times" table on local database.

