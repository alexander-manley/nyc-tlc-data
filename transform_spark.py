import os

from pyspark.sql import SparkSession

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, BooleanType, TimestampType

from pyspark.sql.functions import col, year, month, quarter, round, max

spark = SparkSession.builder.master("local").appName("nyc_tlc_transform").getOrCreate()
sc = spark.sparkContext

# Get the data paths
files = sorted(os.listdir("data"))
paths = [os.path.join("data", file) for file in files]

# Cast correct types
schema = StructType([
    StructField("VendorID", IntegerType()),
    StructField("tpep_pickup_datetime", TimestampType()),
    StructField("tpep_dropoff_datetime", TimestampType()),
    StructField("passenger_count", IntegerType()),
    StructField("trip_distance", DoubleType()),
    StructField("RatecodeID", IntegerType()),
    StructField("store_and_fwd_flag", BooleanType()),
    StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()),
    StructField("payment_type", IntegerType()),
    StructField("fare_amount", DoubleType()),
    StructField("extra", DoubleType()),
    StructField("mta_tax", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("tolls_amount", DoubleType()),
    StructField("improvement_surcharge", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("congestion_surcharge", DoubleType())
])

def get_month_name_from_path(path):
    """
    Get month name from a path

    Examples:
    path = "/tmp/month_01"
    get_month_name_from_path(path)
    """
    basename = os.path.basename(os.path.splitext(path)[0])
    month = basename.split("_")[-1].lstrip('0')
    return month

def clean_data(df, month_name):
    """
    Clean the monthly "Yellow Taxi Trip Records" CSV data

    Details:
    - There is an assumption that the timestamps not belonging to a monthly CSV
      file month are wrong; therefore, they are removed in this step. For
      example, if the 04 (April) CSV file contains a timestamp from the 02
      (February) of the same year, it's considered a wrong value, and removed.

    - All rows where the timestamps not belonging to the 2020 year are
      considered wrong, and removed.  There are some journeys that are started
      on 31-12 and ended on 01-01 of the next year, but they aren't many, and
      there are also removed together with others.
    """
    # Remove rows where the years aren't matching:
    df = df.withColumn('tpep_pickup_datetime_year', year('tpep_pickup_datetime'))\
        .withColumn('tpep_dropoff_datetime_year', year('tpep_dropoff_datetime'))
    df = df.filter((df.tpep_pickup_datetime_year == 2020) & (df.tpep_dropoff_datetime_year == 2020))

    # Remove rows where the months aren't matching:
    df = df.withColumn('tpep_pickup_datetime_month', month('tpep_pickup_datetime'))\
        .withColumn('tpep_dropoff_datetime_month', month('tpep_dropoff_datetime'))
    df = df.filter((df.tpep_pickup_datetime_month == int(month_name)) & (df.tpep_dropoff_datetime_month == int(month_name)))

    # (use inequality signs below because there are some minus values)
    # Remove rows where total_amount isn't greater than zero:
    df = df.filter(df.total_amount > 0)
    # Remove rows where trip_distance isn't greater than zero:
    df = df.filter(df.trip_distance > 0)
    # Remove rows where passenger_count isn't greater than 0:
    df = df.filter(df.passenger_count > 0)
    # Remove rows where fare_amount isn't greater than 0 :
    df = df.filter(df.fare_amount > 0)
    # Remove duplicated rows:
    df = df.distinct()

    return df


def get_data(paths, schema, select_cols=None):
    """
    Get nyc-tlc CSV data as list

    :param paths: paths of the data files.
    :param schema: columns schema.
    :param select_cols: columns to select. If `None`, all columns are brought.
    """
    data = {}
    for path in paths:
        month_name = get_month_name_from_path(path)
        df = spark.read.csv(path, header=True, schema=schema)
        df = clean_data(df, month_name)

        if select_cols is not None:
            df = df.select(select_cols)

        data[month_name] = df
    return data

# ---------- Questions ----------

months = list(range(1, 13))

# Identify in which DOLocationID the tip is highest, in proportion to the cost
# of the ride, per quarter

def calc_tip_percentage(data):
    return round(data["tip_amount"] / data["total_amount"] * 100, 2)

def calc_quarter(data):
    """
    Details:
    Chooses the pickup timestamp as an identifer for the quarter category:
    """
    return quarter(data["tpep_pickup_datetime"])

tip_data_cols = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "tip_amount",
    "total_amount"
]
tip_data = get_data(paths, schema, tip_data_cols)

highest_tip_perc = {}
for month in months:
    month = str(month)
    tip_data[month] = tip_data[month]\
        .withColumn("tipPercentage", calc_tip_percentage(tip_data[month]))
    tip_data[month] = tip_data[month]\
        .withColumn("quarter", calc_quarter(tip_data[month]))

    tip_perc_by_doloc = tip_data[month]\
        .groupBy("DOLocationID", "quarter")\
        .agg(max("tipPercentage").alias("maxTipPercentage"))

    max_tip_perc_by_doloc = tip_perc_by_doloc\
        .sort(tip_perc_by_doloc.maxTipPercentage.desc())\
        .limit(1)

    # eager-loading here because it's more manageable:
    print(f"calculating month: \"{month}\"")
    highest_tip_perc[month] = max_tip_perc_by_doloc.collect()

rdd = sc.parallelize(highest_tip_perc.values())
df = rdd.flatMap(lambda x: x).toDF().distinct()
df = df.sort(df.quarter.asc())

df.write.parquet("output/highest_tip_perc.parquet")

# Identify at which hour of the day the speed of the taxis is highest (speed =
# distance/trip duration)
pass

