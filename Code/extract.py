# Assigmement :
# 1.  Implement the product_data.csv file from S3 system ( AWS ) rather than extracting it from local
# 2. Learn what are the different logging level ( there are 8 logging levels ) ( Info, error, fatal....)
# 3. complete the exception handling for remaining fuctions in extraction

import pandas as pd
import paramiko
import pytest
from sqlalchemy import create_engine
import cx_Oracle
import logging
from Config.config import *


logging.basicConfig(
    filename='Logs/process.log',  # Name of the log file
    filemode='a',  # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO  # Set the logging level
)
logger = logging.getLogger(__name__)


#oracle_engine = create_engine("oracle+cx_oracle://system:admin@localhost:1521/xe")
oracle_engine = create_engine(f'oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}')

#mysql_engine = create_engine("mysql+pymysql://root:Admin%40143@localhost:3308/Retaildwh")
mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')
# extrac the data from sales_data_tobe_deleted.csv and write in to staging_sales table in mysql

class DataExtraction:

    def Sales_Data_From_Linux_Server(self):
        """Fixture to download sales data from a remote Linux server via SSH/SFTP."""
        try:
            # Establish SSH client connection
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # Auto accept unknown keys
            ssh_client.connect({hostname}, username={username}, password={password})
            # Use SFTP to download the file
            sftp = ssh_client.open_sftp()
            sftp.get({remote_file_path} , {local_file_path})
            #print(f"File downloaded successfully from {remote_file_path} to {local_file_path}")
        except Exception as e:
            print(f"An error occurred during SFTP file transfer: {e}")
            raise pytest.fail(f"Test failed due to SSH/SFTP error: {e}")

    def extraction_of_sales_data_file(self):
        logger.info("Data extraction for sales_data_tobe_deleted.csv has started..")
        try:
            df = pd.read_csv("SourceSystems/sales_data_Linux.csv")
            df.to_sql("staging_sales",mysql_engine,if_exists='replace',index=False)
            logger.info("Data extraction for sales_data_tobe_deleted.csv has completed..")
        except Exception as e:
            logger.error("Error encountered while sales_data extrcation",e,exc_info=True)


    def extraction_of_product_data_file(self):
        logger.info("Data extraction for product_data.csv has started..")
        try:
            df = pd.read_csv("SourceSystems/product_data.csv")
            df.to_sql("staging_product",mysql_engine,if_exists='replace',index=False)
            logger.info("Data extraction for product_data.csv has completed..")
        except Exception as e:
            logger.error("Error encountered while product_data extrcation", e, exc_info=True)

    def extraction_of_supplier_data_file(self):
        logger.info("Data extraction for supplier_data.json has started..")
        df = pd.read_json("SourceSystems/supplier_data.json")
        df.to_sql("staging_supplier",mysql_engine,if_exists='replace',index=False)
        logger.info("Data extraction for supplier_data.json has completed..")

    def extraction_of_inventory_data_file(self):
        logger.info("Data extraction for inventory_data.xml has started..")
        df = pd.read_xml("SourceSystems/inventory_data.xml",xpath=".//item")
        df.to_sql("staging_inventoy",mysql_engine,if_exists='replace',index=False)
        logger.info("Data extraction for inventory_data.xml has completed..")

    def extraction_of_stores_data_Oracle_db(self):
        logger.info("Data extraction for stores data has started..")
        SQLquery = """select * from stores"""
        df = pd.read_sql(SQLquery,oracle_engine)
        df.to_sql("staging_stores",mysql_engine,if_exists='replace',index=False)
        logger.info("Data extraction for stores data  has completed..")

extractRef = DataExtraction()
#extractRef.Sales_Data_From_Linux_Server()
logger.info("Dtaa extarction processs started...")
extractRef.extraction_of_sales_data_file()
extractRef.extraction_of_product_data_file()
extractRef.extraction_of_supplier_data_file()
extractRef.extraction_of_inventory_data_file()
extractRef.extraction_of_stores_data_Oracle_db()
logger.info("Dtaa extarction processs successfully completed...")