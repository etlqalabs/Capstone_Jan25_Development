
import pandas as pd
import paramiko
import pytest
from sqlalchemy import create_engine
import cx_Oracle
import logging
from Config.config import *


logging.basicConfig(
    filename='Logs/transformProcess.log',  # Name of the log file
    filemode='a',  # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO  # Set the logging level
)
logger = logging.getLogger(__name__)


#mysql_engine = create_engine("mysql+pymysql://root:Admin%40143@localhost:3308/Retaildwh")
mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')
# extrac the data from sales_data_tobe_deleted.csv and write in to staging_sales table in mysql

class DataTranformation:

    def transform_filter_sales_data(self):
        logger.info("Filter  Transfromatio  has started..")
        try:
            query = """select * from staging_sales where sale_date>='2024-09-10'"""
            df = pd.read_sql(query, mysql_engine)
            df.to_sql("filtered_sales_data", mysql_engine, if_exists='replace', index=False)
            logger.info("Filter Transfromation  has completed..")
        except Exception as e:
            logger.error("Error encountered while filter transformation",e,exc_info=True)

    def transform_router_sales_data_Low(self):
        logger.info("Router- LOW  Transfromatio  has started..")
        try:
            query = """select * from filtered_sales_data where region='Low'"""
            df = pd.read_sql(query, mysql_engine)
            df.to_sql("low_sales", mysql_engine, if_exists='replace', index=False)
            logger.info("Router- LOW Transfromation  has completed..")
        except Exception as e:
            logger.error("Error encountered while Router- LOW transformation",e,exc_info=True)

    def transform_router_sales_data_High(self):
        logger.info("Router- HIGH  Transfromatio  has started..")
        try:
            query = """select * from filtered_sales_data where region='High'"""
            df = pd.read_sql(query, mysql_engine)
            df.to_sql("high_sales", mysql_engine, if_exists='replace', index=False)
            logger.info("Router- HIGH Transfromation  has completed..")
        except Exception as e:
            logger.error("Error encountered while Router- HIGH transformation",e,exc_info=True)

    def transform_aggregator_sales_data(self):
        logger.info("Aggregator  Transfromatio  has started..")
        try:
            query = """select product_id,month(sale_date),year(sale_date),sum(quantity*price) as total_sales 
                        from filtered_sales_data group by product_id,month(sale_date),year(sale_date);"""
            df = pd.read_sql(query, mysql_engine)
            df.to_sql("monthly_sales_summary_source", mysql_engine, if_exists='replace', index=False)
            logger.info("Aggregator Transfromation  has completed..")
        except Exception as e:
            logger.error("Error encountered while Aggregator transformation",e,exc_info=True)

    def transform_aggregator_inventory_level(self):
        logger.info("Aggregator inventory Transfromatio  has started..")
        try:
            query = """select store_id,sum(quantity_on_hand) from staging_inventoy group by store_id;"""
            df = pd.read_sql(query, mysql_engine)
            df.to_sql("aggregated_inventory_level", mysql_engine, if_exists='replace', index=False)
            logger.info("Aggregator inventory Transfromation  has completed..")
        except Exception as e:
            logger.error("Error encountered while Aggregator inventory transformation",e,exc_info=True)


    def transform_joiner_sales_product_store(self):
        logger.info("joiner_sales_product_store Transfromatio  has started..")
        try:
            query = """select fs.sales_id,fs.quantity,fs.sale_date,fs.price,fs.quantity*fs.price as total_sales,
                        p.product_id,p.product_name,s.store_id,s.store_name
                        from filtered_sales_data as fs
                        inner join staging_product as p on p.product_id= fs.product_id
                        inner join staging_stores as s on s.store_id= fs.store_id
                        """
            df = pd.read_sql(query, mysql_engine)
            df.to_sql("sales_with_details", mysql_engine, if_exists='replace', index=False)
            logger.info("joiner_sales_product_storeTransfromation  has completed..")
        except Exception as e:
            logger.error("Error encountered while joiner_sales_product_store transformation",e,exc_info=True)



transformRef = DataTranformation()
logger.info("Dtaa Transfromaiton processs started...")
transformRef.transform_filter_sales_data()
transformRef.transform_router_sales_data_High()
transformRef.transform_router_sales_data_Low()
transformRef.transform_aggregator_sales_data()
transformRef.transform_aggregator_inventory_level()
transformRef.transform_joiner_sales_product_store()
logger.info("Dtaa Transfromaiton processs successfully completed...")