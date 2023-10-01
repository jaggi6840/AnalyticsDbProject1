# Databricks notebook source
data= [("James", "Sales", 2000),
("sofy", "Sales", 3000),
("Laren", "Sales", 4000),
("Kiku", "Sales", 5000),
("Sam", "Finance", 6000),
("Samuel", "Finance", 7000),
("Yash", "Finance", 8000),
("Rabin", "Finance", 9000),
("Lukasz", "Marketing", 10000),
("Jolly", "Marketing", 11000),
("Mausam", "Marketing", 12000),
("Lamba", "Marketing", 13000),
("Jogesh", "HR", 14000),
("Mannu", "HR", 15000),
("Sylvia", "HR", 16000),
("Sama", "HR", 17000),
]

# COMMAND ----------

from pyspark.sql.types import StructType, StructField,StringType,IntegerType
emp_schema = StructType([
             StructField("name",StringType()), 
             StructField("department",StringType()),
             StructField("Salary", IntegerType())
             ])

# COMMAND ----------

df = spark.createDataFrame(data,emp_schema)

# COMMAND ----------

from pyspark.sql.functions import sum,avg,max,min,lead,lag,rank,dense_rank,lead,lag,first,last

# COMMAND ----------

from pyspark.sql.window import Window 
from pyspark.sql.functions import sum,lead,lag,rank,dense_rank,col
# Window Partition on Department
WindowSpecAgg = Window.partitionBy(col('department'))
# Window Partition on Department and Salary
WindowSpec = Window.partitionBy(col('department')).orderBy(col('salary').desc())
# above window is not going to give you result as it follows Range Between Unbounded Preceding and Current Row
WindowSpec1 = Window.partitionBy(col('department')).orderBy(col('salary').desc()).rowsBetween(Window.unboundedPreceding , Window.unboundedFollowing)
#spark.sql("SELECT FULL_NAME,DEPARTMENT , SALARY,HIRE_DATE, SUM(SALARY) OVER(ORDER BY HIRE_DATE ROWS BETWEEN 1 PRECEDING and CURRENT ROW) as SAL FROM  processed.dim_employee_sql_scd1 ").show(n=200)

# COMMAND ----------

from pyspark.sql.functions import sum,avg,max,min,lead,lag,rank,dense_rank,lead,lag,first,last
df2 = df.withColumn('sum' , sum(col('salary')).over(WindowSpecAgg)) \
         .withColumn('average' , avg(col('salary')).over(WindowSpecAgg)) \
         .withColumn('max' , max(col('salary')).over(WindowSpecAgg)) \
         .withColumn('min' , min(col('salary')).over(WindowSpecAgg)) \
         .withColumn('rank' , rank().over(WindowSpec))\
         .withColumn('dens_rank' , dense_rank().over(WindowSpec)) \
         .withColumn('lead' , lead(col('salary'),1,0).over(WindowSpec)) \
         .withColumn('lag' , lag(col('salary'),1,0).over(WindowSpec)) \
         .withColumn('first' , first(col('salary')).over(WindowSpec1)) \
         .withColumn('last' , last(col('salary')).over(WindowSpec1)).show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col,sum,asc
WindowSpec = Window.orderBy(col('salary').cast(IntegerType()).asc()).rowsBetween(Window.unboundedPreceding,0)
WindowSpec1 = Window.orderBy(col('salary').cast(IntegerType()).asc()).rowsBetween(Window.unboundedPreceding,2)
WindowSpec2 = Window.orderBy(col('salary').cast(IntegerType()).asc()).rowsBetween(Window.currentRow  , Window.unboundedFollowing)

# COMMAND ----------

df.withColumn("Running Salary1" , sum(col('salary')).over(WindowSpec)) \
  .withColumn("Running Salary2" , sum(col('salary')).over(WindowSpec1)) \
  .withColumn("Running Salary3" , sum(col('salary')).over(WindowSpec2)).show()
