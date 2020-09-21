import sys, os, shutil
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType



test_data = "C:\\Telco Relax\\Input_test\\"
prod_data = "C:\\Telco Relax\\Input\\"

spark = SparkSession.builder.master("local[1]") \
    .appName('Telco_Relax_ETL') \
    .getOrCreate()

spark.conf.set(
  "",
  "")

data = spark.read.options(inferSchema='True', delimiter=',').csv(prod_data + "*.csv")

desired_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("event_start_time", TimestampType(), True),
    StructField("event_type", StringType(), True),
    StructField("rate_plan_id", IntegerType(), True),
    StructField("billing_flag_1", IntegerType(), True),
    StructField("billing_flat_2", IntegerType(), True),
    StructField("duration", IntegerType(), True),
    StructField("charge", DoubleType(), True),
    StructField("month", StringType(), True)
    ])

#checking schema data types if they match
types_desired = [f.dataType for f in desired_schema.fields]

types_actual = [f.dataType for f in data.schema.fields]

#print(types_desired)
#print(types_actual)

if types_desired != types_actual:
    sys.exit("Schema validation failed")
elif data.count == 0:
    sys.exit("File is empty")
else:
    print("Schema and file validation OK")

print(data.count())
print(data.printSchema())

data_with_schema = data.withColumnRenamed("_c0", "customer_id").withColumnRenamed("_c1", "event_start_time").withColumnRenamed("_c2", "event_type") \
.withColumnRenamed("_c3", "rate_plan_id").withColumnRenamed("_c4", "billing_flag_1").withColumnRenamed("_c5", "billing_flat_2")\
.withColumnRenamed("_c6", "duration").withColumnRenamed("_c7", "charge").withColumnRenamed("_c8", "month")

#parsin date from event start time so it would be possible to partition by it
data_with_date = data_with_schema.withColumn("date", to_date("event_start_time", "yyyy-MM-dd"))

data_with_date.repartition("date", "event_type").write.mode("append").partitionBy("date", "event_type").format("parquet").save("wasbs://datalake@telcorelaxblob01.blob.core.windows.net/data")

#move loaded file to archive folder

dest = "C:\\Telco Relax\\Archive\\processed_" + date.today().strftime('%Y-%m-%d')

files = os.listdir(prod_data)

for f in files:
    shutil.move(test_data + f, dest + f)
