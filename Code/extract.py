import pandas as pd
import paramiko
import pytest
from sqlalchemy import create_engine
import cx_Oracle

oracle_engine = create_engine("oracle+cx_oracle://system:admin@localhost:1521/xe")
mysql_engine = create_engine("mysql+pymysql://root:Admin%40143@localhost:3308/Retaildwh")

# extrac the data from sales_data_tobe_deleted.csv and write in to staging_sales table in mysql

class DataExtraction:

    def Sales_Data_From_Linux_Server(self):
        """Fixture to download sales data from a remote Linux server via SSH/SFTP."""
        try:
            # Establish SSH client connection
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # Auto accept unknown keys
            ssh_client.connect('192.168.0.111', username='etlqalabs', password='root')
            # Use SFTP to download the file
            sftp = ssh_client.open_sftp()
            sftp.get('/home/etlqalabs/sales_data.csv' , 'SourceSystems/sales_data_Linux.csv')
            #print(f"File downloaded successfully from {remote_file_path} to {local_file_path}")
        except Exception as e:
            print(f"An error occurred during SFTP file transfer: {e}")
            raise pytest.fail(f"Test failed due to SSH/SFTP error: {e}")

    def extraction_of_sales_data_file(self):
        print("Data extraction for sales_data_tobe_deleted.csv has started..")
        df = pd.read_csv("SourceSystems/sales_data_Linux.csv")
        df.to_sql("staging_sales",mysql_engine,if_exists='replace',index=False)
        print("Data extraction for sales_data_tobe_deleted.csv has completed..")

    def extraction_of_product_data_file(self):
        print("Data extraction for product_data.csv has started..")
        df = pd.read_csv("SourceSystems/product_data.csv")
        df.to_sql("staging_product",mysql_engine,if_exists='replace',index=False)
        print("Data extraction for product_data.csv has completed..")

    def extraction_of_supplier_data_file(self):
        print("Data extraction for supplier_data.json has started..")
        df = pd.read_json("SourceSystems/supplier_data.json")
        df.to_sql("staging_supplier",mysql_engine,if_exists='replace',index=False)
        print("Data extraction for supplier_data.json has completed..")

    def extraction_of_inventory_data_file(self):
        print("Data extraction for inventory_data.xml has started..")
        df = pd.read_xml("SourceSystems/inventory_data.xml",xpath=".//item")
        df.to_sql("staging_inventoy",mysql_engine,if_exists='replace',index=False)
        print("Data extraction for inventory_data.xml has completed..")

    def extraction_of_stores_data_Oracle_db(self):
        print("Data extraction for stores data has started..")
        SQLquery = """select * from stores"""
        df = pd.read_sql(SQLquery,oracle_engine)
        df.to_sql("staging_stores",mysql_engine,if_exists='replace',index=False)
        print("Data extraction for stores data  has completed..")

extractRef = DataExtraction()
extractRef.Sales_Data_From_Linux_Server()
extractRef.extraction_of_sales_data_file()
'''
extractRef.extraction_of_product_data_file()
extractRef.extraction_of_supplier_data_file()
extractRef.extraction_of_inventory_data_file()
extractRef.extraction_of_stores_data_Oracle_db()
'''
