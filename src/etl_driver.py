from pyspark.sql import SparkSession
from etl_helper import ETLTransform
from s3_helper import S3Module
from pathlib import Path
import logging
import logging.config
import configparser
from data_warehouse_start.pipeline_start import WarehouseDriver
import time

# Setting configurations. Look config.cfg for more details
config = configparser.ConfigParser()
config.read_file(open(f'{Path(__file__).parents[0]}/config.yml'))

# Setting up logger, Logger properties are defined in logging.ini file
logging.config.fileConfig(f'{Path(__file__).parents[0]}/logging.ini')
logger = logging.getLogger(__name__)



def create_sparksession():
    """
    Initialize a spark session
    """
    return SparkSession.builder.master('yarn').appName('') \
           .config('','') \
           .config('', '') \
           .enableHiveSupport().getOrCreate()


def main():

    logging.debug("\n\nSetting up Spark Session...")
    spark = create_sparksession()
    etlt = ETLTransform(spark)

    # Modules in the project
    modules = {

    }

    logging.debug("\n\nCopying data from s3 landing zone to ...")
    s3 = S3Module()
    s3.s3_move_data(source_bucket= config.get('',''), target_bucket= config.get('', ''))

    files_in_working_zone = gds3.get_files(config.get('', ''))

    # Cleanup processed zone if files available in working zone
    if len([set(modules.keys()) & set(files_in_working_zone)]) > 0:
        logging.info("Cleaning up processed zone.")
        s3.clean_bucket(config.get('', ''))

    for file in files_in_working_zone:
        if file in modules.keys():
            modules[file]()

    logging.debug("Waiting before setting up Warehouse")
    time.sleep(5)

    # Starting warehouse functionality
    warehouse = WarehouseDriver()
    logging.debug("Setting up staging tables")
    warehouse.setup_staging_tables()
    logging.debug("Populating staging tables")
    warehouse.load_staging_tables()
    logging.debug("Setting up Warehouse tables")
    warehouse.setup_warehouse_tables()
    logging.debug("Performing UPSERT")
    warehouse.perform_upsert()


# Entry point for the pipeline
if __name__ == "__main__":
    main()