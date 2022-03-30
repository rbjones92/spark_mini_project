# Robert Jones
# 3.18.22
# Try # 2 - Spark mini project

import findspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import *
from pyspark.sql.functions import col

findspark.init()
spark = SparkSession.builder.getOrCreate()

car_schema = StructType([
 StructField('incident_id',IntegerType(),True),
 StructField('incident_type',StringType(),True),
 StructField('vin_number',StringType(),True),
 StructField('make',StringType(),True),
 StructField('model',StringType(),True),
 StructField('year',StringType(),True),
 StructField('Incident_date',StringType(),True),
StructField('notes',StringType(),True)
 ])

df = spark.read.format('csv').options(Header=False).load('C:\\BigDataLocalSetup\\Spark\\Spark_Projects\\Spark_Mini_Project\\data2.csv',schema=car_schema)

trimmed_df = df.where('make is not null and model is not null')

unique_vins = trimmed_df.select('vin_number','make','model')

data_collect = unique_vins.collect()



def fill_nulls():
    df.withColumn("make", when(col("vin_number") == data_collect[0][0], data_collect[0][1]))


rdd2=df.rdd.map(fill_nulls)

