from Code.extract import DataExtraction
from Code.load import DataLoading
from Code.transform import DataTranformation

import pandas as pd
import paramiko
import pytest
from sqlalchemy import create_engine
import cx_Oracle
import logging
from Config.config import *


logging.basicConfig(
    filename='Logs/Extractprocess.log',  # Name of the log file
    filemode='a',  # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO  # Set the logging level
)
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    extractRef = DataExtraction()
    # extractRef.Sales_Data_From_Linux_Server()
    logger.info("Dtaa extarction processs started...")
    extractRef.extraction_of_sales_data_file()
    extractRef.extraction_of_product_data_file()
    extractRef.extraction_of_supplier_data_file()
    extractRef.extraction_of_inventory_data_file()
    extractRef.extraction_of_stores_data_Oracle_db()
    logger.info("Dtaa extarction processs successfully completed...")

    transformRef = DataTranformation()
    logger.info("Dtaa Transfromaiton processs started...")
    transformRef.transform_filter_sales_data()
    transformRef.transform_router_sales_data_High()
    transformRef.transform_router_sales_data_Low()
    transformRef.transform_aggregator_sales_data()
    transformRef.transform_aggregator_inventory_level()
    transformRef.transform_joiner_sales_product_store()
    logger.info("Dtaa Transfromaiton processs successfully completed...")
    LoadRef = DataLoading()
    logger.info("Data Loading processs started...")
    LoadRef.load_fact_sales_table()
    LoadRef.load_fact_inventory_table()
    LoadRef.load_monthly_sales_summary_table()
    LoadRef.load_inventory_level_by_store_table()
    logger.info("Data Loading processs completed...")