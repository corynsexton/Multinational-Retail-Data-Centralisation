# METHODS TO CLEAN DATA FROM EACH OF THE DATA SOURCES

from data_extraction import db_extractor
import numpy as np
import pandas as pd
import re

class DataCleaning():

    def clean_user_data(self):
        # EXTRACT TABLE
        df_legacy_users = db_extractor.read_rds_table('legacy_users')
        # SETS INDEX COLUMN AND INDEX
        df_legacy_users.set_index('index', inplace = True)
        # SETS COLUMNS TO DATETIME CATEGORY
        df_legacy_users['date_of_birth'] = pd.to_datetime(df_legacy_users['date_of_birth'], errors='coerce')
        df_legacy_users['date_of_birth'] = df_legacy_users['date_of_birth'].dt.strftime('%Y-%m-%d')
        df_legacy_users['join_date'] = pd.to_datetime(df_legacy_users['join_date'], errors='coerce')
        df_legacy_users['join_date'] = df_legacy_users['join_date'].dt.strftime('%Y-%m-%d')
        # CORRECTS GB SPELLING AND CHANGES COLUMN TYPE TO CATEGORY
        df_legacy_users['country_code'] = df_legacy_users['country_code'].str.replace('GGB', 'GB')
        df_legacy_users['country_code'] = df_legacy_users['country_code'].astype('category')
        # DROPS NULL ROWS
        df_legacy_users = df_legacy_users.dropna()

        return df_legacy_users


    def clean_card_data(self):
        df_card_details = db_extractor.retrieve_pdf_data()

        # REPLACE NULL VALUES WITH NAN
        df_card_details.replace('NULL', np.NaN, inplace=True)
        # CHANGE DATE COLUMNS TO DATETIME & REMOVE TIMESTAMP FROM COLUMN
        df_card_details['date_payment_confirmed'] = pd.to_datetime(df_card_details['date_payment_confirmed'], errors='coerce')
        # SETS DATE TO FORMAT OF CHOICE - REMOVES TIMESTAMP
        df_card_details['date_payment_confirmed'] = df_card_details['date_payment_confirmed'].dt.strftime('%Y-%m-%d')      
        # REMOVE ANY NON-NUMERIC CHARACTERS FROM COLUMN
        df_card_details['card_number'] = df_card_details['card_number'].astype(str)
        df_card_details['card_number'] = df_card_details['card_number'].str.replace('?', '')
        # MAKE SURE CARD DIGITS ARE ALL CORRECT FOR EACH PROVIDER AND REMOVE ANY THAT AREN'T
        card_providers = {'American Express':[15], 'Mastercard':[16], 'VISA 13 digit':[13], 'VISA 16 digit':[16], 'Diners Club / Carte Blanche':[14], 
                            'VISA 19 digit':[19], 'JCB 16 digit':[16], 'Maestro':range(16-19), 'JCB 15 digit':[15], 'Discover':[16]}
        # SET ANY INCORRECT ROWS TO NAN
        df_card_details['card_provider'] = df_card_details['card_provider'].apply(lambda x : x if x in card_providers else np.NaN) 
        # DROP INCORRECT ROWS
        df_card_details = df_card_details.dropna()

        return df_card_details
 

    def clean_store_data(self):
        df_store_data = db_extractor.retrieve_stores_data()

        # SET INDEX
        df_store_data.set_index('index', inplace=True)
        # CLEAN COUNTRY CODE & SET TO CATEGORY
        country_codes = ['GB', 'DE', 'US']
        df_store_data['country_code'] = df_store_data['country_code'].apply(lambda x : x if x in country_codes else np.NaN)
        # CLEAN CONTINENTS
        continents = ['Europe', 'America']
        df_store_data['continent'] = df_store_data['continent'].apply(lambda x : x if x in continents 
                                                                      else ('Europe' if 'Europe' in str(x) 
                                                                            else ('America' if 'America' in str(x) 
                                                                                  else np.NaN)))
        # CLEAN STORE TYPE
        store_types = ['Mall Kiosk', 'Super Store', 'Local', 'Outlet', 'Web Portal']
        df_store_data['store_type'] = df_store_data['store_type'].apply(lambda x : x if x in store_types else np.NaN)
        # SET TYPE TO CATEGORY
        category_type = {'continent':'category', 'country_code':'category', 'store_type':'category'}
        df_store_data = df_store_data.astype(category_type)
        # SET OPENING DATE TO DATETIME % REMOVE TIMESTAMP
        df_store_data['opening_date'] = pd.to_datetime(df_store_data['opening_date'], errors = 'coerce')    # ERRORS = 'COERCE' SETS ANY NON-CONVERTIBLE VALUES TO NAN
        df_store_data['opening_date'] = df_store_data['opening_date'].dt.strftime('%Y-%m-%d')
        # DROP LAT COLUMN. NO VALID DATA
        df_store_data = df_store_data.drop('lat', axis=1)
        # ROUND LONGITUDE AND LATITUDE COLUMNS
        df_store_data[['longitude', 'latitude']] = df_store_data[['longitude', 'latitude']].apply(lambda x : round(pd.to_numeric(x, errors = 'coerce'), 2 ))
        # SET STORE_CODE COLUMN TO PARTICULAR FORMAT USING REGEX
        df_store_data['store_code'] = df_store_data['store_code'].apply(lambda x: x if re.match('^[A-Z]{2,3}-[A-Z0-9]{8}$', str(x)) else np.nan)
        # REMOVE ANY NON-NUMERIC CHARACTERS FROM COLUMN
        df_store_data['staff_numbers'] = df_store_data['staff_numbers'].str.extract('([\d.]+)')
        # CLEAN ADDRESS COLUMN - NEED TO REPLACE '\n' WITH ', '
        df_store_data['address'] = df_store_data['address'].replace({'\n':', '}, regex=True)
        # DROP NULL VALUE ROWS
        df_store_data.replace('NULL', np.NaN, inplace=True)
        df_store_data = df_store_data.dropna()

        return df_store_data


    def convert_product_weights(self, df_products_data):
            # CONVERTS WEIGHT COLUMN INTO STRING TYPE
            df_products_data['weight'] = df_products_data['weight'].astype(str) 
            # CONVERTS WEIGHTS INTO CORRECT VALUES
            df_products_data.replace({'weight':['3 x 132g', '12 x 100g', '12 x 85g', '6 x 400g', '40 x 100g', '6 x 412g', '16 x 10g', '3 x 2g', '4 x 400g', '3 x 90g', '2 x 200g', '8 x 150g', '8 x 85g', '5 x 145g']}, 
                                       {'weight':['396g', '1200g', '1020g', '2400g', '4000g', '2472g', '160g', '6g', '1600g', '270g', '400g', '1200g', '680g', '725g']}, inplace=True)
            # ADDS 'UNITS' COLUMN AND FILTERS BY LETTERS ONLY
            df_products_data['units'] = df_products_data['weight'].apply(lambda x : ''.join(y for y in x if not y.isdigit()))
            # SEPARATES NUMBERS FROM LETTERS AND CONVERTS TO FLOAT
            df_products_data['weight'] = df_products_data['weight'].str.extract('([\d.]+)').astype(float)
            # CONVERT UNITS TO KG
            df_products_data['weight'] = df_products_data.apply(lambda x: x['weight'] / 1000 if x['units']=='g' or x['units']=='ml' else x['weight'], axis=1)
            df_products_data['weight'] = df_products_data.apply(lambda x: x['weight'] * 0.0283495 if x['units'] == 'oz' else x['weight'], axis=1)
            df_products_data['weight'] = df_products_data.apply(lambda x: x['weight'] * 0.453592 if x['units'] =='lb' else x['weight'], axis=1)
            # DROP UNITS COLUMN
            df_products_data.drop(columns='units', inplace=True)
            
            return df_products_data


    def clean_products_data(self, df_products_data):
        df_products_data = data_cleaner.convert_product_weights(df_products_data)

        # DROP UNNECESSARY/DUPLICATE COLUMN
        df_products_data = df_products_data.drop('Unnamed: 0', axis=1)
        # STRIPS OUT CURRENCY AND CONVERTS COLUMN TO FLOAT
        df_products_data['product_price'] = df_products_data['product_price'].str.extract('([\d.]+)').astype(float)
        # CORRECTS TYPO IN REMOVED CATEGORY COLUMN
        df_products_data['removed'] = df_products_data['removed'].replace('Still_avaliable', 'Still_available')
        # SETS VALID CATEGORIES
        removed_categories = ['Removed', 'Still_available'] 
        df_products_data['removed'] = df_products_data['removed'].apply(lambda x : x if x in removed_categories else np.NaN)
        category_categories = ['pets', 'toys-and-games', 'sports-and-leisure', 'health-and-beauty', 'homeware', 'food-and-drink', 'diy']
        df_products_data['category'] = df_products_data['category'].apply(lambda x: x if x in category_categories else np.NaN)
        # SETS COLUMNS TO CATEGORY
        category_types = {'category' : 'category', 'removed' : 'category'}
        df_products_data = df_products_data.astype(category_types)
        # SETS COLUMN TO DATE_TIME
        df_products_data['date_added'] = pd.to_datetime(df_products_data['date_added'], errors = 'coerce')
        df_products_data['date_added'] = df_products_data['date_added'].dt.strftime('%Y-%m-%d')
        # DROP NULL VALUES
        df_products_data.replace('NULL', np.NaN, inplace=True)
        df_products_data = df_products_data.dropna()
        # RENAME COLUMNS
        df_products_data = df_products_data.rename(columns={'product_price':'product_price_£','weight':'weight_kg'})
        
        return df_products_data
    

    def clean_orders_data(self):
        # EXTRACTS ORDERS DATA
        df_orders_data = db_extractor.read_rds_table('orders_table')
        # REMOVES UNWANTED COLUMNS
        df_orders_data = df_orders_data.drop(columns = ['first_name', 'last_name', '1', 'level_0'])
        # SETS INDEX 
        df_orders_data.set_index('index', inplace=True)

        return df_orders_data


    def clean_sales_data(self):
        df_sales_data = db_extractor.extract_from_s3(s3_address='https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')

        #SET CATEGORY
        period_category = ['Morning', 'Midday', 'Evening', 'Late_Hours']
        df_sales_data['time_period'] = df_sales_data['time_period'].apply(lambda x : x if x in period_category else np.NaN)
        df_sales_data['timestamp'] = pd.to_datetime(df_sales_data['timestamp'], errors = 'coerce')
        # DROPS NULL VALUES
        df_sales_data.replace('NULL', np.NaN, inplace=True)
        df_sales_data = df_sales_data.dropna()
        df_sales_data = df_sales_data.astype({'time_period':'category', 'month':'category', 'year':int, 'day':int})
      
        return df_sales_data


data_cleaner = DataCleaning() 


# CALLING UPLOAD_TO_DB METHOD TO UPLOAD CLEANED DATA TO PGADMIN
# db_connector.init_db_engine()
# db_connector.list_db_tables()

# UPLOAD CLEANED USER DATA TO SALES_DATA
# cleaned_user_data = data_cleaner.clean_user_data()
# db_connector.upload_to_db(cleaned_user_data, 'dim_users')

# UPLOAD CLEANED CARD DETAILS TO SALES_DATA
# cleaned_card_data = data_cleaner.clean_card_data()
# db_connector.upload_to_db(cleaned_card_data, 'dim_card_details')

# UPLOAD CLEANED STORE DATA TO SALES_DATA
# cleaned_store_data = data_cleaner.clean_store_data()
# db_connector.upload_to_db(cleaned_store_data, 'dim_store_details')

# UPLOAD CLEANED PRODUCTS DATA TO SALES_DATA
# df_products_data = data_cleaner.convert_product_weights(df_products_data = db_extractor.extract_from_s3(s3_address='s3://data-handling-public/products.csv'))
# cleaned_products_data = data_cleaner.clean_products_data(df_products_data)
# db_connector.upload_to_db(cleaned_products_data, 'dim_products')

# UPLOAD CLEANED ORDERS DATA TO SALES_DATA
# cleaned_orders_data = data_cleaner.clean_orders_data()
# cleaned_orders_data
# db_connector.upload_to_db(cleaned_orders_data, 'orders_table')

# UPLOAD CLEANED SALES DATA
# cleaned_sales_data = data_cleaner.clean_sales_data()
# db_connector.upload_to_db(cleaned_sales_data, 'dim_date_times')