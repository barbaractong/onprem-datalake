import os
import shutil


from datetime import datetime

from services.GetData import ImportDataService

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

get_data_from_origin = ImportDataService('../data-origin')


def files(path):
    for file in os.listdir(path):
        if os.path.isfile(os.path.join(path, file)):
            yield file


get_data_from_origin.load_zip_files()

for prep_csv in files('../data-container/stage'):
    temp_prep_csv = spark.read.csv(f'../data-container/stage/{prep_csv}', sep='|', inferSchema=True, header=True,
                                   encoding='UTF-8')

    temp_prep_csv.withColumn("DT_CARGA", F.current_date())
    schema = temp_prep_csv.schema

    temp_prep_csv.write.option("compression", "snappy").option("schema", schema) \
        .parquet(f'../data-container/raw/{os.path.splitext(prep_csv)[0]}.parquet')

    shutil.move(f'../data-container/stage/{prep_csv}', '../data-container/stage/moviment_ingestion_history/')
