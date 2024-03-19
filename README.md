# Multinational Data Centralisation Project

## Table of Contents
[1. Introduction](#Introduction)
[2. Database Connection](#Database-Connection)
[3. Data Extraction](#Data-Extraction)
[4. Data Cleaning](#Data-Cleaning)
[5. Star Schema](#Star-Schema)
[6. Data Analysis - SQL Queries](#Data-Analysis)
____
## 1. Introduction
Imagine you are part of a multinational retail company that sells goods across the globe.

However, there's an issue... All of your sales data is widely spread across many different data sources such as an AWS RDS Database, AWS S3 and CSV, JSON and PDF files, making it difficult to review and analyse the data easily and accurately.

This project produces a system that extracts, cleans and stores all of your company's data from the different data types into one centralised location being a PostgreSQL database which acts as a single source of truth for the sales data. I have used pgAdmin in this project as a management tool.

The new database allows for your company to produce elaborate analysis and queries with accurate results which we will look at using SQL at the end of this project.
____


## 2. Database Connection
To begin, I created a Python script called `database_utils.py` with a `DatabaseConnector()` class containing methods which allow me to access the database connections that I require throughout the project.

I have two `.yaml` files hidden in my `.gitignore` containing credentials for the two databases I use. One database contains data which I will extract and clean. The other database is one I have created called Sales_Data which is where I will be sending all of the cleaned data to once it has been cleaned up. This is the centralised location where all the data will be stored ready for analysis.

_It is worth noting here that it is impoprtant to store any personal details or security information in your .gitignore file for security reasons._

### Sales_Data Database
`local_db_creds.yaml` contains credentials for the new Sales_Data database that I have created especially for this project. As mentioned above, this is the database that I will send the data to once cleaned.

Firstly, I have created a method within `DatabaseConnector()` (also referred to as `db_connector` througout my code) to read the contents of the yaml file. This method is called `read_local_db_creds()` and uses `open()` and `yaml.safeload()` to return the credentials.

<img width="1045" alt="read_local_db_creds" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/9cd6f54c-5714-47ae-8ce0-a5f12a62c988">

Now that I have my credentials, I need a method which will connect to my database. This method is called `upload_to_db()`. I will use `create_engine()` which is an in-built method of the SQLalchemy library to create an engine using the returned credentials from `read_local_db_creds()`. 

I also need to send the cleaned data to this PostgreSQL database so I will use `to_sql()` to do this. The method is named `upload_to_db()` and takes in the dataframe I want to upload to the database as an argument, along with the name I wish to give the table in pgAdmin.

<img width="1266" alt="upload_to_db" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/d82c16b7-6b89-4760-b66c-b3db913cc4a1">

### AWS RDS Database
`db_creds.yaml` contains credentials of the first data source I am going to extract data from - an AWS RDS Database.

Similarly to above, I have a method which reads the yaml file called `reads_db_creds()` and returns the credentials for the RDS database.
These credentials are then fed into my next method called `init_db_engine()`. The purpose of this method is to solely create an engine, again using `create_engine()`, to connect me to the database where I can then access the data.

<img width="1038" alt="read_db_creds" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/77645bc1-cc75-4018-8844-06b848ac52f1">

There are a number of different tables within this AWS database and so I created a method to list all of the different tables so I know which one I needed to retrieve data from. The method `list_db_tables()` does this by connecting to the database by using my `init_db_engine()` method and then using `inspect()` followed by `.get_table_names()` to `print()` all of the table names that the database contains. 

With this knowledge, I can now move onto extracting the data from whichever table I need.
<img width="1059" alt="list_db_tables" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/2d345877-4987-4a3b-a3a8-9277f20cf834">
<img width="1223" alt="table_names_result" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/3c0e349c-d6be-4fc1-b56a-f1a687b8ed06">

For this section, I needed to `pip install` the libraries `sqlalchemy` and `yaml` and use a couple of imports to access the methods used.

<img width="980" alt="database_utils_import" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/d91f01ee-b65c-4d18-b196-b999e1f05b2d">

____
## 3. Data Extraction
Now that my database connections are set up, I am all set to extract all of the datasets I need. I have created another Python script called `data_extraction.py` which contains a class called `DataExtractor()`, also referred to as `db_extractor` throughout my code. Within this class, I have created a number of methods capable of extracting data from various data forms. The goal is to extract the data and convert it to a Pandas Dataframe so that it can be cleaned.

The imports needed for this section are as follows:

<img width="1138" alt="data_extractor_import" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/808df132-ed52-40d3-af88-d36d0a1c68f0">

### Data Sources
* AWS RDS Database
* PDF document
* API
* AWS S3 Bucket - CSV file
* AWS S3 Bucket - JSON file

#### AWS RDS Database
From this database, I am going to extract two sources of data from the tables, user data and orders data.

Using the table name results from `list_db_tables()` in `database_utils.py`, I now have the table names that sit within the RDS Database. 

I have created a method called `read_rds_table()` which takes in the `table_name` of the data we want to extract. It will be `legacy_users` to gather our user data, and `orders_table` to gather the sales order data. Entering the `table_name` as an argument, I then used `pd.read_sql_table()` to convert the data to a pandas dataframe.

Using this method twice throughout the project provided me with two sets of data which are now ready to be cleaned - user and sales order data.

<img width="1196" alt="read_rds_table" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/60121b18-5d81-4c81-b583-3aa5711c28ed">

##### User Data:

<img width="1129" alt="user_data_result" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/af5b37c8-b62c-4d24-8b53-96be92b0f61b">

##### Orders Data:

<img width="1131" alt="orders_table_result" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/f52d57a6-35c9-4a49-b168-aca3244014bb">

#### PDF Document
I have a PDF file which contains card detail data which I need to extract and clean so that is can contribute to the final analysis. I created a method within my `db_extractor` class called `retrieve_pdf_data()` to extract data from a PDF document. This takes in the PDF link as an argument, assigned to variable `card_details_pdf` in this case.

I installed and imported the Python package `tabula-py` so that I could use `read_pdf()`.

I then converted the data into a Pandas Dataframe and our card data is now ready to be cleaned.

<img width="1102" alt="retrieve_pdf_data" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/2bc02e2d-b54a-42e1-a996-36348056f68e">

##### Card Data:

<img width="1110" alt="card_detail_results" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/c3214e68-607b-4254-8791-cee5b3b3d0b6">

#### API
To get the store data, I need to extract it through an API. First, I must connect to the API and so my method takes in two arguments - an endpoint and a header dictionary. The API key will be the header dictionary.

Before extracting the data, I created a method called `list_number_of_stores()` to find out how many stores the API can provide data for.
This uses the `get()` method within the `requests` module and `loads()` method within `json`.

<img width="1212" alt="list_number_of_stores" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/fa8a7670-5bd5-4a6d-be5e-b4ff7a55e164">

The result was 451 which told me that there are 451 stores that I can extract data for. I then fed this information into my next method which will return all 451 stores' data as one pandas dataframe. I have called this method `retrieve_stores_data()`.
<img width="1203" alt="retrieve_stores_data" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/2a52a434-85f9-443f-b38a-0965fb20c9c2">

As you can see above, I created an empty list `[]` in order for the stores' data to be stored as the method runs through each one individually.

##### Stores Data:

<img width="1283" alt="store_data_result" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/1993d36e-ab39-47c9-9ed8-7594229841b4">

#### AWS S3 Bucket - CSV, JSON
The last two data sources I need to extract are both from an AWS S3 Bucket where I will find product data in a CSV file and sales date data in a JSON file.

To access AWS S3, I need to have a log in set up so that I am able to generate an Access and Secret Key. My keys are stored in `admin_aws_keys.yaml` which again has been uploaded to my `.gitignore` file.

I have created a method called `extract_from_s3()` which takes in the `s3_address` as an argument.
First, the method reads my `admin_aws_keys.yaml` file and so it has now used the all important keys that are needed to access AWS S3.

Next, I used the `client()` method from the `boto3` module which I have installed and imported.
Importantly, I have included two `if` and `elif` statements to split the string of the `s3_address`. This will decifer between a CSV file and a JSON file.
<img width="1246" alt="extract_from_s3" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/434f28ba-7604-4638-92a3-16b3e5f7c735">

##### Products Data:

<img width="1260" alt="products_data_result" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/b14c9dba-b873-4481-84f3-ce1d1929c39e">

##### Sales Dates Data:

<img width="1261" alt="date_times_results" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/39664f89-702f-4460-97f5-2f6ce4183221">

___

## 4. Data Cleaning
Now that I have extracted all of the data from various different datatypes, I need to clean the data so that it will give accurate results when analysed. I will do this by removing NULL values, checking datatypes, ensuring there are no invalid entries in categorised columns and many more cleaning techniques which we will dig into in this section.

I created a class called `DataCleaning()` which contains all of my cleaning methods for each different dataset. I fed each pandas dataframe that I previously extracted into their own cleaning method. Once cleaned, the data will then be ready to be sent to my `Sales_Data` Database using my `upload_to_db()` method in `db_connector`.

#### User Data
<img width="1043" alt="clean_user_data" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/7c47765b-90ee-47ef-b4d9-4190a381e3e2">

I begin by asigning the user dataframe to a variable, `df_legacy_users`. 

I dealt with the index column first as the user data has two identical columns and so I set the `index` column as the index which removes the initial index column. The user data has two date columns and so I ensure they are set to datetime type by using `to_datetime()` followed by `dt.strftime()` to set them to the specific format I want.

The `country_code` column is going to be a category column and so I used `set(df_legacy_users['country_code'])` to see what those country categories would be. The result brought a data error to my attention - where users have entered 'GGB' instead of 'GB'. I used `.str.replace()` to correct any of these typing errors to 'GB' and used `.astype()` to convert the column to datatype 'category'.

Finally, I dropped all rows which contained NULL values before returning my cleaned dataframe.

#### Card Data
<img width="1343" alt="clean_card_data" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/8de51f85-8982-4722-91ad-dc43afffc76b">

The card data contains NULL values, so I start by using `.replace()` to change any of these NULL values to `NaN` using the Numpy library I have imported as `np`.
I noticed that some of the card numbers in the `card_number` column contain characters such as `?`, but of course I want all characters to be numeric, and so these must go. I used `.replace()` to remove all question marks.

There are certain card providers within this dataset and so I wanted to remove any rows that are not within the `card_providers` category that I have created. To create the `card_providers` category, I used a dictionary stating the number of digits a card number should have for that particular card provider. I then used the `lambda` function to iterate through all of the rows and set all other rows that are not within the card provider category to `NaN`. 

I removed these unnecessary entries in the dataset by dropping all rows that contain `NaN` values using `.dropna()`.

#### Store Data
<img width="1361" alt="clean_store_data" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/36d39130-16fb-4039-aec4-6d8cc5202b8a">

The store data contains many different columns. The first thing I notice is that there are two `lat` columns and so I use `.drop('lat', axis=1)` to drop one of these columns as it contains no useful data.

The `longitude` and `latitude` columns that remain are a bit messy and so I want to round them to 2 decimal places. I do this by using the lambda function to iterate through the rows, using `pd.to_numeric()` to set the values as numbers whilst finally including the `round()` function and specifying `2` to set all values to 2 d.p.

The store data contains columns such as `country_code`, `continents` and `store_type` which need to be type `category` and so I do this the same way as the card data by using `.apply()` and the `lambda` function. This set any values that do not fit the categories I specified in my lists to `NaN`.

The `store_code` column needs to follow a specific format and so I use `re.match()` and regular expressions also known as regex to enforce this.

`staff_number` column should contain numeric values only and so I used `.str.extract('([\d.]+)')` to remove any non-numeric characters. `.str` converts the entries to a string so that I can iterate through the string and extract any non-numeric characters using another regex expression.

`address` contains a lot of `\n`. This means a new line. However, as I am working with a pandas dataframe, I used `.replace()` to replace any `\n` with a comma and a space to make the addresses readable for the company.

Finally, I replaced any `NULL` values with `NaN` and dropped any rows containing `NaN` values using `.dropna()` to provide more accurate data.

#### Product Data
The product dataset contains a weight column which gives us values in all different units such as kilograms (kg), grams (g), millilitres (ml), ounces (oz), pounds (lb) and so I have created a method specially for cleaning the weight column.

<img width="1369" alt="convert_product_weights" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/10ec7eda-4b34-42cf-bcb0-ef760fd242de">

First, I used `.replace()` and a dictionary to replace any wild data in the `weight` column so that the string represented one number and its units. For example, `12 x 100g` would be replaced with `1200g`. There were a few entries like this and so I used the `lambda` function again to iterate through all rows replacing results where necessary.

Now that all entries in the `weight` column are presented as I need, I next stripped out the unit and created a column for these called `units`. I perfromed this by using `lambda` and `.isdigit()`.

I used `.extract()` to separate numeric and non-numeric characters, and changed the `weight` column to `float` type.

The last thing I wanted to do is make sure all the weights represent the same unit, kg. Once `weight` is all in kg, I drop the `units` column as it is no longer needed.

<img width="1347" alt="clean_product_data" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/2390f85e-7271-4633-b14d-539e7e20614d">

I can now feed this new product data into another method to clean the remainder of the data.

I dropped the `Unnamed: 0` column as it is a duplicate index column. Followed by stripping out the currency character in the `product_price` column, setting to `float` type and renaming the column to `product_price_£` so that the currency is still visible to the company.

The `removed` column needs to be a category column containing either 'Removed' or 'Still_available'. However, there is a typo and the column states `Still_avaliable` instead of `Still_available` and so I corrected this using `.replace()` and set the column to type `category`.

I changed the `date_added` column using `.to_datetime()` and `dt.strftime()` and dropped `NaN` value rows.

I also did a check using `.isnumeric()` to ensure all characters in the `EAN` column were numeric.

#### Order Data
<img width="1322" alt="clean_orders_data" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/16273a1a-3730-493b-a2d3-624c2d7a9b38">

The orders table contains a few columns which we do not provide us with any helpful data. The `first_name` and `last_name` column mostly state 'None' rather than names, the column named `1` contains only `NaN` values and `level_0` column looks like an additional index column. 

Therefore, I dropped all of these columns and set the `index` column as the index. I used `set_index()` to do this.

#### Sales Datatime Data
<img width="1344" alt="clean_sales_data" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/e1763ae3-2e1a-48fb-ad02-7516a56f841e">

Last but not least, the sales datetime data. This dataframe contains data on the date and times of sales transactions. Therefore, it is important that the `timestamp` column is set to datetime type. I used `pd.to_datetime()` here to do this.

I specified the `time_period` categories in the list `period_category` that I have created and set any values that are not in this list to `NaN`.

I changed the `time_period` and `month` columns to `category`, and the `year` and `day` columns to `int`.

Finally, I changed any NULL values to `NaN` and dropped any rows containing these.
___
## 5. Star Schema
All the data has now been cleaned and stored in one location - the Sales Data Database that I created at the beginning. However, before I can start querying the data, I need to set up the database schema and I am going to use SQL within pgAdmin 4 to manage this. 

The database schema is important as it establishes relationships between the tables.

#### Altering Datatypes
I updated each table by using `ALTER TABLE` AND `ALTER COLUMN` to set each column to the correct datatypes using `TYPE`. I have used a number of types such as `VARCHAR()`, `DATE`, `FLOAT`, `SMALLINT`, `UUID` and `BOOL`.

I am going to use the products data table as an example below, as this required more altering than others.

By using `UPDATE`, I added a `weight_class` column in the `dim_products` table so that it would be easier to see those products that are Light, Medium, Heavy or Truck Required. This would help the company plan transportation methods. 

I used a `CASE` statement to do this which was also used when setting the `removed` column to boolean type. I renamed this column to `Still_available` using `RENAME` so it would show `True` if there are products available for purchase, and `False` if they are out of stock.

<img width="1274" alt="alter_products_table" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/eb0711a0-77cf-49e5-9073-4783be99a943">

#### Primary Keys
The final stage of preparing the data for analysis, is ensuring the Primary and Foreign Keys are set.

I have added a Primary Key to each table. Each table contains at least one column that the `orders_table` contains. It is important that I assign these particular columns as the Primary Keys.

<img width="1184" alt="primary_keys" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/d33b7b83-bdf3-4990-ba14-d6eb5793ad2b">

#### Foreign Keys
As the `orders_table` is the source of truth for our data, it is the main table that all of the other tables will serve with their additional information. Therefore, I have assigned Foreign Keys to the columns within the `orders_table`. 

This will finalise the linking of all tables to secure our database schema.

<img width="1220" alt="foreign_keys" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/7901e498-ff68-4819-8d8e-cdb932843a78">

____

## 6. Data Analysis - SQL Queries
Now that I have the database schema set up, I can finally start analysing the data and producing accurate results for the company! 
Below demonstrates a number of queries the company wants me to look into using the data I have collated.

#### Query 1: How many stores does the company have and in which countries?
Focusing on the store data table, I used the `COUNT()` aggregate to count the number of stores and used `GROUP BY country_code` to get the total number of stores for each country. I then used `ORDER BY total_no_stores DESC` to filter with the hightest total first. If I didn't specify`DESC` here, SQL would default to filtering in ascending order, with the lowest total first.

<img width="1243" alt="task1" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/46049381-fc56-471a-8b3d-f7e0ebd549bf">

##### Result: 
The United Kingdom has 266 stores, Germany has 141 stores, whilst the United States has only 34 stores.

<img width="1010" alt="task1_result" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/9ab40c55-c551-400d-9d6e-97763ff4005e">

##### Query 2: Which locations currently have the most stores?
The company wants to close some stores before opening more in other locations. I looked at the store data again for this query, but this time I used the locality column, rather than the countries column.

As there are stores in lots of different areas across the world, I used `LIMIT` here to only produce the top 7 areas with the most stores for the company to consider closure of.

<img width="1292" alt="task2" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/8cb6179d-7cf7-4207-8374-81c541054920">

##### Result:

Chapletown has the most stores with 14 in the area.

<img width="1007" alt="task2_result" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/9cd8fdf9-8f8d-49ef-b508-23ea7df7c558">

#### Query 3: Which months produced the largest amount of sales?
First, I need a way to work out the total sales as we do not currently have any figures in any of our tables that provides this. I did this by multiplying the `product_quantity` in the `orders_table` by the `product_price_£` in the `dim_products` table. To do this in postgresql, I used `JOIN` to join the two tables together as shown below.

I then used the `SUM()` aggregate to sum the column to get the `total_sales_£`. I have also used the `ROUND()` function to round the total sales to 2 d.p.

The final step is to `GROUP BY` the month column as this is the information the company requested, order by the `total_sales_£` in descending order, and `LIMIT` to the top 6.
<img width="1034" alt="task3" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/1650804e-8f24-4e4d-a693-1146d3ae8019">

##### Result:
Month 8 producted the most sales of £673k. Followed by Month 1 with £668k.

<img width="1012" alt="task3_result" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/d218117a-fe27-4f9b-b063-1dd67d737a35">

#### Query 4: How many sales are coming from online?
I need to replicate the total sales column from the previous query, joining the `dim_products` and `orders_table` tables again. 

I made a `CASE` statement, which ran a `WHEN` statement for when the store is a `'Web Portal'`. 
This was to find the total of online sales, compared to those in-store.

<img width="1029" alt="task4" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/e7290e96-cfeb-4438-ba75-25e22a677f73">

##### Result:
26,957 sales are made online selling 107,739 products. 
Whilst 93,166 sales are made in stores.

<img width="1013" alt="task4_result" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/8107f92f-7f75-4047-ab38-6c009bb4ff84">

#### Query 5: What percentage of sales come through each type of store?
I will `JOIN` the `dim_products` table with the `orders_table` to get the `total_sales_£` again. When working with multiple tables, it is easier to alias the table names to shorten the code. I have used `AS` here so readers can clearly see what has happened. However, `AS` is not necessary as it will alias the table name without it which yu will see in the next query and throughout the rest of my code.

I have then come up with a formula to give us the percentage of total sales. 

Outside this formula, I have used `CAST()`. I have used this as it allows me to convert the `total_sales_£` column to a numeric type so that I can get the correct percetange values I require. 

Finally, I have grouped by `store_type`.

<img width="1194" alt="task5" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/db4e0921-9852-4685-905b-76ec80f2c272">

##### Result:
A large 44.56% of sales come from Local stores, bringing in a total of £3.4m. 
The lowest sales comes from Outlet stores at 8.18% and Mall Kiosks at 9.05%.

<img width="1297" alt="task5_result" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/e30d1778-3fe1-47b6-92ac-2230c14ad9f2">

#### Query 6: Which month in each year produced the highest cost of sales?
After finding the total sales and joining `dim_date_times`, `dim_products` and `orders_table`, I have grouped by the `year` column in `dim_date_times` and ordered by `total_sales_£` in descending order.

<img width="1186" alt="task6" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/6b3301a1-3c93-4344-8b80-cb8568a3bff2">

##### Result:
Month 3 in 1994 produced the largest amount of sales over all the years.

<img width="1301" alt="task6_result" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/a26a9ee3-e594-4ed7-bf57-a68837cdbb5f">

#### Query 7: What is the staff headcount?
Working with the store data, I have used the `SUM()` aggregate to find out the total number of staff members. 

To break it down, I have then used `GROUP BY country_code` to show us how many staff are employed in each country.

<img width="1203" alt="task7" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/d356bde8-f9d2-4471-a08c-b26b8b6fd0f4">

##### Result:
13,307 staff are employed in the UK, 6,087 in Germany, and 1,384 in the US. 
This ties in with the number of stores in each country as the UK has the most, and the US the least.

<img width="1246" alt="task7_result" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/6a9acfe7-e000-4c6b-b679-d0ee4cabea8d">

#### Query 8: Which German store type is selling the most?
To ensure the data I am providing is for German stores only, I have used the `WHERE` clause to only look at the data whose `country_code` is `DE` (which is the abbreviation for Germany in this database.)

<img width="1202" alt="task8" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/09e1f878-0437-48e7-b9a3-8284e1f50adb">

##### Result:
The Local stores are selling the most in Germany by a huge amount at £1.1m. 
The second highest store type is Super Stores making sales of £385k.

<img width="1287" alt="task8_result" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/48268934-ce1b-4980-b291-6d9b0f676f92">

#### Query 9: How quickly is the company making sales?
The aim of this query is to find the average time difference between each sale and group this by `year` so that the company can see which years made the fastest sales. I have used CTEs in this query for ease of readability.

First, I made a CTE called `correct_timestamp` which you can see the result of below. This CTE contains the `year`, `month`, `day` columns, along with the time element from the `timestamp` column. I also added a column called `date_time` which combined the date and time for each sale. This has now given me a correct datetime column I can work with.

<img width="1137" alt="correct_timestamp" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/110394f1-ac97-47d2-8727-d8035a7a2bde">

From `correct_timestamp`, I then made a second CTE called `next_sale`. I need the next sale time to compare to the previous sale time to be able to work out the difference between the sale times. I did this by using `ORDER BY date_time` to filter the data from earliest to latest. I then used `LEAD` to get the `next_sale` time, along with `PARTITION BY year` as I will be looking at the average time difference for each year. I used the `next_sale` column to work out the `time_difference` between each sale.

<img width="1113" alt="next_sale" src="https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/02371b7f-f49e-4b12-86f9-7f8f5b32c5e9">

Now that I have the time difference between each sale, I created one last CTE called `time_difference` to clearly collate the data I need - `year` and `time_difference`. From this, I can select the data I need. I used the `AVG()` aggregate and `GROUP BY year` to produce the average time difference between each sale per year.

![task9](https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/363a7f12-8f90-44ef-87f4-3ad9df49ad2b)

##### Result:
All years have an average of over 2 hours between each sale, with 2013 producing the slowest sales.

![task9_result](https://github.com/corynsexton/Multinational-Retail-Data-Centralisation/assets/117574774/87facdf1-7ce2-456d-b08d-c2c9f2233195)

