# Robert Jones
# 3.18.22
# Try # 2 - Spark mini project



from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import *
from pyspark.sql.functions import col

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

trimmed_df = df.where('make IS NOT null and model IS NOT null and year IS NOT null')

unique_vins = trimmed_df.select('vin_number','make','model','year')

data_collect = unique_vins.collect()

make_mapper = {row.vin_number: {"make": row.make} for row in data_collect}
year_mapper = {row.vin_number: {"year": row.year} for row in data_collect}

df = df.na.fill('none')

def fill_nulls(x):

    vin = x.vin_number
    make = x.make
    year = x.year
    incident = x.incident_type

    if make == 'none':
        make = make_mapper[vin]['make']
    
    if year == 'none':
        year = year_mapper[vin]['year']

    return (incident,vin,make,year)

rdd2 = df.rdd.map(lambda x: fill_nulls(x))

rdd2 = rdd2.toDF(['incident','vin','make','year'])

result = rdd2.groupBy('make','year','incident').count().where(col('incident') == 'A')
result.show()





