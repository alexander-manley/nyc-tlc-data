import os
import tempfile
import logging

from pyspark.sql import SparkSession

import pandas
from sqlalchemy import create_engine

spark = SparkSession.builder.master("local").appName("nyc_tlc_parquet").getOrCreate()

db_connection_url = "postgresql://docker:12345@localhost:5432/nyc_tlc"
engine = create_engine(db_connection_url)

def create_tempfile():
    temp = tempfile.NamedTemporaryFile()
    return temp.name

def write_csv(df, path):
    logging.info(f"writing csv into \"{path}\"")
    df.write\
        .option("header","true")\
        .option("sep",",")\
        .mode("overwrite")\
        .csv(path)

def get_csv_file(path):
    csv = [file for file in os.listdir(path) if file.endswith(".csv")][0]
    return os.path.join(path, csv)

def import_csv_into_pg(folder, csv_file):
    tbl_name = folder.split(".")[0]
    csv_df = pandas.read_csv(csv_file)
    csv_df.to_sql(tbl_name, engine, index=False)
    logging.info("written table \"{tbl_name}\"")

def main():
    folders = sorted(os.listdir("output"))

    for folder in folders:
        path = os.path.join("output", folder)
        temp_fpath = create_tempfile()
        df = spark.read.parquet(path)
        write_csv(df, temp_fpath)
        csv_file = get_csv_file(temp_fpath)
        import_csv_into_pg(folder, csv_file)

if __name__ == "__main__":
    main()
