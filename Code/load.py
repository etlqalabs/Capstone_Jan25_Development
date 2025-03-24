
import pandas as pd
import pytest
from sqlalchemy import create_engine, text
import logging
from Config.config import *


logging.basicConfig(
    filename='Logs/LoadProcess.log',  # Name of the log file
    filemode='a',  # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO  # Set the logging level
)
logger = logging.getLogger(__name__)

#mysql_engine = create_engine("mysql+pymysql://root:Admin%40143@localhost:3308/Retaildwh")
mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')

class DataLoading:
    def load_fact_sales_table(self):
        query = text("""insert into fact_sales(sales_id,product_id,store_id,quantity,total_sales,sale_date)  
                        select sales_id,product_id,store_id,quantity,total_sales,sale_date from sales_with_details;
                        """)
        try:
            with mysql_engine.connect() as conn:
                logger.info("Fact_sales table load started...")
                logger.info(query)
                conn.execute(query)
                conn.commit()
                logger.info("Fact_sales table load completed...")
        except Exception as e:
            logger.error("Error encountered while loading fact_sales",e,exc_info=True)


    def load_fact_inventory_table(self):
        query = text("""insert into fact_inventory(product_id,store_id,quantity_on_hand,last_updated)  
                        select product_id,store_id,quantity_on_hand,last_updated from staging_inventoy;
                        """)
        try:
            with mysql_engine.connect() as conn:
                logger.info("Fact_inventory table load started...")
                logger.info(query)
                conn.execute(query)
                conn.commit()
                logger.info("Fact_inventory table load completed...")
        except Exception as e:
            logger.error("Error encountered while loading Fact_inventory",e,exc_info=True)


    def load_monthly_sales_summary_table(self):
        query = text("""insert into monthly_sales_summary(product_id,month,year,total_sales)
                        select product_id,month,year,total_sales from monthly_sales_summary_source;""")
        try:
            with mysql_engine.connect() as conn:
                logger.info("monthly_sales_summary table load started...")
                logger.info(query)
                conn.execute(query)
                conn.commit()
                logger.info("monthly_sales_summary table load completed...")
        except Exception as e:
            logger.error("Error encountered while loading monthly_sales_summary",e,exc_info=True)

    def load_inventory_level_by_store_table(self):
        query = text("""insert into inventory_levels_by_store(store_id,total_inventory)
                        select store_id,total_inventory from aggregated_inventory_level;""")
        try:
            with mysql_engine.connect() as conn:
                logger.info("inventory_level_by_store_ table load started...")
                logger.info(query)
                conn.execute(query)
                conn.commit()
                logger.info("inventory_level_by_store_ table load completed...")
        except Exception as e:
            logger.error("Error encountered while loading inventory_level_by_store_",e,exc_info=True)


if __name__ == "__main__":
    LoadRef = DataLoading()
    logger.info("Data Loading processs started...")
    LoadRef.load_fact_sales_table()
    LoadRef.load_fact_inventory_table()
    LoadRef.load_monthly_sales_summary_table()
    LoadRef.load_inventory_level_by_store_table()
    logger.info("Data Loading processs completed...")