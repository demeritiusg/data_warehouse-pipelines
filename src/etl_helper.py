from pyspark.sql.types import StringType
from pyspark.sql import functions as fn
import spark_helper
import logging
import configparser
from pathlib import Path

logger = logging.getLogger(__name__)

config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/config.yml"))

class ETLTransform:

    def __init__(self, spark):
        self._spark = spark
        self._load_path = 's3a://' + config.get('', '')
        self._save_path = 's3a://' + config.get('', '')


    def transform_dataset(self):
        logging.debug("Inside transform dataset module")
		#TODO change dataframe name
        unformed_df = \
            self._spark.read.csv( self._load_path + '/raw.csv', header=True, mode='PERMISSIVE',inferSchema=True)

        lookup_df = raw_df.groupBy('id')\
                            .agg(fn.max('record_create_timestamp').alias('record_create_timestamp'))
        lookup_df.persist()
        fn.broadcast(lookup_df)

        deduped_df = raw_df\
                            .join(lookup_df, ['id', 'record_create_timestamp'], how='inner')\
                            .select(raw_df.columns) \
                            .withColumn('name', spark_helper.remove_extra_spaces('name'))

        logging.debug(f"Attempting to write data to {self._save_path + '/'}")
        deduped_df\
            .repartition(10)\
            .write\
            .csv(path = self._save_path + '/', sep = '|', mode='overwrite', compression='gzip', header=True, timestampFormat = 'yyyy-MM-dd HH:mm:ss.SSS', quote = '"', escape = '"')
