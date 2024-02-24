# CONNECT AND UPLOAD DATA TO THE DATABASE

import yaml
from sqlalchemy import create_engine, inspect


class DatabaseConnector():

    # READS db_creds.yaml & RETURNS CREDENTIALS AS DICTIONARY
    def read_db_creds(self):               
        with open('db_creds.yaml', 'r') as f:
            read_db_creds = yaml.safe_load(f)
        return read_db_creds                    

    # CREATES DATABASE ENGINE
    def init_db_engine(self):
        db_creds = self.read_db_creds()

        self.db_type = 'postgresql'
        self.db_api = 'psycopg2'
        self.db_host = db_creds['RDS_HOST']
        self.db_password = db_creds['RDS_PASSWORD']       
        self.db_user = db_creds['RDS_USER']
        self.db_database = db_creds['RDS_DATABASE']
        self.db_port = db_creds['RDS_PORT']
        
        engine = create_engine(f"{self.db_type}+{self.db_api}://{self.db_user}:{self.db_password}@{self.db_host}/{self.db_database}")
        return engine

    
    # LIST ALL TABLES IN DATABASE
    def list_db_tables(self):
        engine = self.init_db_engine()
        inspector = inspect(engine)
        
        for table_name in inspector.get_table_names():
                print(table_name)

    
   # READS LOACL DATABASE CREDS             
    def read_local_db_creds(self):               
        with open('local_db_creds.yaml', 'r') as f:
            read_local_db_creds = yaml.safe_load(f)
        return read_local_db_creds                    

     
    # UPLOADS CLEANED DATA TO NEW SALES_DATA DATABASE
    def upload_to_db(self, dataframe, table_name):
        local_db_creds = self.read_local_db_creds()
        local_engine = create_engine(f"{local_db_creds['LOCAL_DATABASE_TYPE']}+{local_db_creds['LOCAL_DB_API']}://{local_db_creds['LOCAL_USER']}:{local_db_creds['LOCAL_PASSWORD']}@{local_db_creds['LOCAL_HOST']}:{local_db_creds['LOCAL_PORT']}/{local_db_creds['LOCAL_DATABASE']}")
        dataframe.to_sql(table_name, local_engine, if_exists='replace', index=False)
        


db_connector = DatabaseConnector()
# db_connector.read_local_db_creds()
# db_connector.init_local_db_engine()
# db_connector.list_db_tables()
# db_connector.upload_to_db(dataframe=data_cleaner.clean_user_data, table_name='legacy_users')
