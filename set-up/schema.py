# Databricks notebook source
# MAGIC %md
# MAGIC   |Table|Content|
# MAGIC   |--|--|
# MAGIC   |01|This NoteBook will be used to store Schema definition|
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StructField,IntegerType,StringType
Taxi_Zone_Schema = StructType([
                   StructField("LocationID",StringType(),True),
                   StructField("Borough",StringType(),True),
                   StructField("Zone",StringType(),True),
                   StructField("Service_zone",StringType(),True)
])

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StructField,IntegerType,StringType,DateType
Calendar_Schema = StructType([
                  StructField("date_key",IntegerType(),True ),
                  StructField("date",DateType(),True ),
                  StructField("year",IntegerType(),True ),
                  StructField("month",IntegerType(),True ),
                  StructField("day",IntegerType(),True ),
                  StructField("day_name",StringType(),True ),
                  StructField("day_of_year",IntegerType(),True ),
                  StructField("week_of_month",IntegerType(),True ),
                  StructField("week_of_year",StringType(),True ),
                  StructField("month_name",StringType(),True ),
                  StructField("year_month",IntegerType(),True ),
                  StructField("year_week",IntegerType(),True )
])

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StructField,IntegerType,StringType
Vendor_Schema = StructType([
                   StructField("vendor_id",IntegerType(),True),
                   StructField("vendor_name",StringType(),True)
])

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StructField,IntegerType,StringType
Trip_Type_Schema = StructType  ([
                   StructField("trip_type",IntegerType() , True),
                   StructField("trip_type_desc",StringType() , True)
])

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StructField,IntegerType,StringType
Rate_Code_schema = StructType([StructField("rate_code", StringType(), False),
                                      StructField("rate_code_id", IntegerType(), True)
                                     ])

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StructField,IntegerType,StringType
Payment_Type_schema = StructType([StructField("payment_type", IntegerType(), False),
                                      StructField("payment_type_desc", StringType(), True)
                                     ])

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StructField,IntegerType,StringType
Payment_Type_Desc_schema = StructType(fields=[StructField("sub_type", IntegerType(), False),
                                             StructField("value", StringType(), True)
                                     ])

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StructField,IntegerType,StringType
Payment_Type_Array_schema = StructType(fields=[
                                      StructField("payment_type", IntegerType(), False),
                                      StructField("payment_type_desc", Payment_Type_Desc_schema)
                                     ])

# COMMAND ----------

from pyspark.sql.types import DateType, StructType,StructField,StructField,IntegerType,StringType,FloatType,datetime,DoubleType,DayTimeIntervalType
Green_Trip_Schema = StructType([
                      StructField("VendorID"  ,IntegerType() , True),
                      StructField("lpep_pickup_datetime" ,DayTimeIntervalType() , True),
                      StructField("lpep_dropoff_datetime" ,DayTimeIntervalType() , True),
                      StructField("store_and_fwd_flag" ,StringType() , True),
                      StructField("RatecodeID" ,IntegerType() , True),
                      StructField("DOLocationID" ,IntegerType() , True),
                      StructField("passenger_count" ,IntegerType() , True),
                      StructField("trip_distance" ,DoubleType() , True),
                      StructField("fare_amount" ,DoubleType() , True),
                      StructField("extra" ,DoubleType() , True),
                      StructField("mta_tax" ,DoubleType() , True),
                      StructField("tip_amount" ,DoubleType() , True),
                      StructField("tolls_amount" ,DoubleType() , True),
                      StructField("ehail_fee" ,IntegerType() , True) ,
                      StructField("improvement_surcharge" ,DoubleType() , True),
                      StructField("total_amount" ,DoubleType() , True),
                      StructField("payment_type" ,IntegerType() , True),
                      StructField("trip_type" ,IntegerType() , True),
                      StructField("congestion_surcharge",DoubleType() , True),
                      StructField("month",IntegerType() , True)
                                     ])

# COMMAND ----------


