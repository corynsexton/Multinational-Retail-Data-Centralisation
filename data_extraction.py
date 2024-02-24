# Methods extract data from different data sources such as CSV files, an API and an S3 bucket

import pandas as pd
from database_utils import db_connector
import tabula
import requests
import json
import boto3
import yaml
import re


class DataExtractor():

    def __init__(self):
        self.engine = db_connector.init_db_engine()
        # self.table_name = db_connector.list_db_tables()
       
    # EXTRACT DATABASE TABLE TO A PANDAS DATAFRAME
    def read_rds_table(self, table_name):
        self.df_table_name = pd.read_sql_table(f"{table_name}", self.engine)

        return self.df_table_name

    # NEEDS TO TAKE IN LINK AS ARGUMENT AND RETUN A PANDAS DATAFRAME
    def retrieve_pdf_data(self, 
                          card_details_pdf='https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'):
        card_details = tabula.read_pdf(card_details_pdf, pages = 'all')
        card_details = pd.concat(card_details)

        return card_details
    
    # RETRIEVES NUMBER OF STORES FROM API - 451 RESULT
    def list_number_of_stores(self,
                           endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores',
                           header = {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}):
        response = requests.get(endpoint, headers=header).text  # CONVERTS TO TEXT
        store_number = json.loads(response)                    # CONVERTS TO JSON
        store_number = store_number['number_stores']          # RETRIEVES ONLY NUMBER OF STORES

        return store_number

    # RETRIEVES STORE DATA FROM LIST OF STORES
    def retrieve_stores_data(self,
                                #  num_of_stores=list_number_of_stores,
                                endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details',
                                header = {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}):
        all_stores = []         # EMPTY LIST TO STORE EACH STORE'S DATA AS IT LOOPS THROUGH ALL 451 STORE ENDPOINTS (RESULT FROM list_number_of_stores METHOD)  
        num_of_stores = self.list_number_of_stores()
        
        for store_number in range(0, num_of_stores):
            stores = requests.get(f'{endpoint}/{store_number}', headers=header)
            response = stores.json()   # RETURNS AS DICTIONARIES   
            all_stores.append(response) # ADDS ALL STORES TO LIST

        df_stores = pd.DataFrame.from_dict(all_stores)  # CONVERTS TO DATAFRAME

        return df_stores
    

    def extract_from_s3(self, s3_address):
        with open('admin_aws_keys.yaml', 'r') as f:
            aws_keys = yaml.safe_load(f)
        access_key = aws_keys['ACCESS_KEY']
        secret_key = aws_keys['SECRET_KEY']
        s3 = boto3.client('s3', 
                          aws_access_key_id=access_key, 
                          aws_secret_access_key=secret_key)
        # s3://data-handling-public/products.csv
        # https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json
        if 's3://' in s3_address:
            s3_address = s3_address.replace('s3://', '')
        elif 'https://' in s3_address:
            s3_address = s3_address.replace('https://', '')
    
        s3_bucket = re.split('/|\\.', s3_address)[0]
        s3_key = s3_address.split('/')[1]
        response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
        
        if '.csv' in s3_address:
            df_s3_data = pd.read_csv(response['Body'])
        elif '.json' in s3_address:
            df_s3_data = pd.read_json(response['Body'])
        
        return df_s3_data
        


db_extractor = DataExtractor()

# df_legacy_users = db_extractor.read_rds_table('legacy_users')
# df_orders_table = db_extractor.read_rds_table('orders_table')
# card_details = db_extractor.retrieve_pdf_data(card_details_pdf='https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
# list_number_of_stores = db_extractor.list_number_of_stores(endpoint='https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', 
#                                                            header_dictionary={'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'})
# store_data = db_extractor.retrieve_stores_data(num_of_stores=list_number_of_stores,
#                                                endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}',
#                                                header_dictionary = {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'})
# db_extractor.list_number_of_stores()
# db_extractor.retrieve_stores_data()
# db_extractor.extract_from_s3(s3_address='s3://data-handling-public/products.csv')
# db_extractor.extract_from_s3(s3_address='https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')


