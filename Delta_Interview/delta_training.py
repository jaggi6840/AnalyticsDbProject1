# Databricks notebook source
# MAGIC %md 
# MAGIC ### DELTA TABLES
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC #### A) CREATE DELTA TABLES

# COMMAND ----------

# MAGIC %md 
# MAGIC 1.**SQL METHOD**

# COMMAND ----------

# MAGIC %sql 
# MAGIC --SHOW DATABASES
# MAGIC --CREATE DATABASE DELTA
# MAGIC USE DELTA

# COMMAND ----------

# %sql 
# DROP DATABASE DELTA CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC create TABLE IF NOT EXISTS DELTA.Employee (
# MAGIC Emp_id Integer,
# MAGIC Emp_name VARCHAR(50),
# MAGIC Gender VARCHAR(50),
# MAGIC Salary Integer
# MAGIC )
# MAGIC PARTITIONED BY (eMP_iD)
# MAGIC LOCATION '/mnt/analyticsdbhub/delta/Employee'

# COMMAND ----------

# MAGIC %md 
# MAGIC 2. **PYSPARK METHOD**

# COMMAND ----------

from delta.tables import *
DeltaTable.createOrReplace(spark) \
.tableName("Employee1") \
.addColumn("Emp_id", "Integer") \
.addColumn("Emp_name" , "String" )\
.addColumn("Gender" , "String" )\
.addColumn("Salary" , "Integer") \
.location('/mnt/analyticsdbhub/delta/Employee1') \
.execute()

# COMMAND ----------

DeltaInstance = DeltaTable.forPath(spark,'/mnt/analyticsdbhub/delta/Employee1')

# COMMAND ----------

DeltaInstance1 = DeltaTable.forName(spark,'Employee1')

# COMMAND ----------

# MAGIC %md
# MAGIC 3. **Dataframe Method**

# COMMAND ----------

Data = [(1,'jaggi','M',10000),
(2,'Mark','M',11000),
(3,'John','M',13000),
(4,'James','M',14000),
(5,'Charlie','M',10000),
(6,'Rink','F',15000),
(7,'Carol','F',9000),
(8,'Steve','M',9000),
(9,'Sara','F',7000),
(10,'Sumit','M',11000),
(11,'Jason','M',12000) ]

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,StringType,IntegerType
Employee_Schema = StructType([
                  StructField("Emp_id" , StringType()),
                  StructField("Emp_name" , StringType()),
                  StructField("Gender" , StringType()),
                  StructField("Salary" , StringType())
                            ])

# COMMAND ----------

df =spark.createDataFrame(Data,Employee_Schema)

# COMMAND ----------

df.write.format("Delta") \
         .mode("Overwrite") \
         .option("path","/mnt/analyticsdbhub/delta/Employee3") \
         .saveAsTable("Employee3")

# COMMAND ----------

# MAGIC %md
# MAGIC #### B) INSERT DATA INTO DELTA TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC 1. **SQL INSERT METHOD**

# COMMAND ----------

# MAGIC %sql 
# MAGIC INSERT INTO DELTA.EMPLOYEE VALUES(1,'jaggi','M',10000);
# MAGIC INSERT INTO DELTA.EMPLOYEE VALUES(2,'Mark','M',11000);
# MAGIC INSERT INTO DELTA.EMPLOYEE VALUES(3,'John','M',13000);
# MAGIC INSERT INTO DELTA.EMPLOYEE VALUES(4,'James','M',14000);
# MAGIC INSERT INTO DELTA.EMPLOYEE VALUES(5,'Charlie','M',10000);
# MAGIC INSERT INTO DELTA.EMPLOYEE VALUES(6,'Rink','F',15000);
# MAGIC INSERT INTO DELTA.EMPLOYEE VALUES(7,'Carol','F',9000);
# MAGIC INSERT INTO DELTA.EMPLOYEE VALUES(8,'Steve','M',9000);
# MAGIC INSERT INTO DELTA.EMPLOYEE VALUES(9,'Sara','F',7000);
# MAGIC INSERT INTO DELTA.EMPLOYEE VALUES(10,'Sumit','M',11000);
# MAGIC INSERT INTO DELTA.EMPLOYEE VALUES(11,'Jason','M',12000);

# COMMAND ----------

# MAGIC %md
# MAGIC 2. **Dataframe Method**

# COMMAND ----------

df.write.insertInto("employee1",overwrite=False)

# COMMAND ----------

# MAGIC %md
# MAGIC 3. **Temp Method**

# COMMAND ----------

df.createOrReplaceTempView("temp_employee1")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO employee3 SELECT * FROM temp_employee1

# COMMAND ----------

# MAGIC %md
# MAGIC 4. **Spark SQL Method**

# COMMAND ----------

spark.sql("INSERT INTO employee3 SELECT * FROM temp_employee1")

# COMMAND ----------

# MAGIC %md 
# MAGIC ####C) SELECT DATA FROM DELTA TABLES 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 1. **SELECT DATA**

# COMMAND ----------

# MAGIC %sql select * from Employee

# COMMAND ----------

# MAGIC %md
# MAGIC 2. **SPARK SQL**

# COMMAND ----------

spark.sql("select * from Employee")

# COMMAND ----------

# MAGIC %md
# MAGIC 3. **SPARK SQL VIA LOCATION**

# COMMAND ----------

# MAGIC %sql select * from DELTA.`/mnt/analyticsdbhub/delta/Employee`

# COMMAND ----------

# MAGIC %md
# MAGIC #### D) DELETE DATA FROM DELTA TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC 1. **SPARK SQL**

# COMMAND ----------

# MAGIC %sql delete from employee where emp_id=2

# COMMAND ----------

# MAGIC %md
# MAGIC 2. **SQL USING DELTA LOCATION (Only for Delta Lake tables )**

# COMMAND ----------

# MAGIC %sql DELETE FROM delta.`/mnt/analyticsdbhub/delta/Employee` where emp_id = 3

# COMMAND ----------

# MAGIC %md
# MAGIC 3. **SQL USING SPARK**

# COMMAND ----------

spark.sql("delete from employee where emp_id=3")

# COMMAND ----------

# MAGIC %md
# MAGIC 4. **Multiple condition using SQL Predicate**

# COMMAND ----------

DeltaInstance.delete("emp_id=5 and gender = 'M'")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### E) UPDATE  DATA FOR DELTA TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC 1. **SQL USING SPARK**

# COMMAND ----------

# MAGIC %sql UPDATE employee SET SALARY=15000 where emp_name='jaggi'

# COMMAND ----------

# MAGIC %md 
# MAGIC 2. **SQL using Delta location**

# COMMAND ----------

# MAGIC %sql UPDATE  delta.`/mnt/analyticsdbhub/delta/Employee` SET SALARY=200000 where emp_name='Jason'

# COMMAND ----------

# MAGIC %md 
# MAGIC 3.**SPARK SQL**

# COMMAND ----------

spark.sql("update  employee SET SALARY=20000 where emp_name='Sumit'")

# COMMAND ----------

# MAGIC %md
# MAGIC 4. **Pyspark Table Instance**

# COMMAND ----------

DeltaInstance1.update(
    condition = "emp_name = 'Sara'",
    set= {"salary" : "300000"}
)

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM EMPLOYEE

# COMMAND ----------

# MAGIC %md
# MAGIC ### F) HISTORY IN DELTA TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY EMPLOYEE;

# COMMAND ----------

# MAGIC %md 
# MAGIC 1. **VERSION**

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM EMPLOYEE VERSION AS OF 11

# COMMAND ----------

# MAGIC %md 
# MAGIC 2. **TIMESTAMP**

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM EMPLOYEE TIMESTAMP AS OF '2023-10-13T18:23:16.000+0000'

# COMMAND ----------

# MAGIC %md 
# MAGIC 3. **VERSION SQL WITH FILE PATH**

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM delta.`/mnt/analyticsdbhub/delta/Employee` VERSION AS OF 11

# COMMAND ----------

# MAGIC %md 
# MAGIC 4. **TIMESTAMP SQL WITH FILE PATH**

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM delta.`/mnt/analyticsdbhub/delta/Employee`  TIMESTAMP AS OF '2023-10-13T18:23:16.000+0000'

# COMMAND ----------

# MAGIC %md 
# MAGIC 5. **PYSPARK TIMESTAMP  WITH FILE PATH**

# COMMAND ----------

df = spark.read.format("Delta") \
               .option("timestampAsOf" , '2023-10-13T18:23:16.000+0000') \
               .load('/mnt/analyticsdbhub/delta/Employee') 

# COMMAND ----------

# MAGIC %md 
# MAGIC 6. **PYSPARK VERSION  WITH FILE PATH**

# COMMAND ----------

df = spark.read.format("Delta") \
               .option("VersionAsOf" , '11') \
               .load('/mnt/analyticsdbhub/delta/Employee') 

# COMMAND ----------

# MAGIC %md 
# MAGIC 7. **PYSPARK   WITH TABLE NAME**

# COMMAND ----------

df = spark.read.format("Delta") \
               .option("VersionAsOf" , '11') \
               .table('employee') 

# COMMAND ----------

# MAGIC %md 
# MAGIC ### G) RESTORE DELTA TABLE

# COMMAND ----------

# MAGIC %md
# MAGIC 1. **RESTORE USING VERSION**

# COMMAND ----------

# MAGIC %sql 
# MAGIC RESTORE TABLE employee TO TIMESTAMP AS OF '2023-10-13T18:25:45.000+0000'

# COMMAND ----------

# MAGIC %sql 
# MAGIC RESTORE TABLE employee TO VERSION AS OF 31

# COMMAND ----------

# MAGIC %md 
# MAGIC ### H) OPTIMIZE DELTA TABLE

# COMMAND ----------

# MAGIC %sql  OPTIMIZE EMPLOYEE

# COMMAND ----------

# MAGIC %md 
# MAGIC ### H) VACCUM DELTA TABLE

# COMMAND ----------

# MAGIC %sql 
# MAGIC INSERT INTO DELTA.EMPLOYEE VALUES(1,'jaggi','M',10000);
# MAGIC INSERT INTO DELTA.EMPLOYEE VALUES(2,'Mark','M',11000);
# MAGIC INSERT INTO DELTA.EMPLOYEE VALUES(3,'John','M',13000);
# MAGIC INSERT INTO DELTA.EMPLOYEE VALUES(4,'James','M',14000);
# MAGIC INSERT INTO DELTA.EMPLOYEE VALUES(5,'Charlie','M',10000);
# MAGIC INSERT INTO DELTA.EMPLOYEE VALUES(6,'Rink','F',15000);
# MAGIC INSERT INTO DELTA.EMPLOYEE VALUES(7,'Carol','F',9000);
# MAGIC INSERT INTO DELTA.EMPLOYEE VALUES(8,'Steve','M',9000);
# MAGIC INSERT INTO DELTA.EMPLOYEE VALUES(9,'Sara','F',7000);
# MAGIC INSERT INTO DELTA.EMPLOYEE VALUES(10,'Sumit','M',11000);
# MAGIC INSERT INTO DELTA.EMPLOYEE VALUES(11,'Jason','M',12000);

# COMMAND ----------

# MAGIC %sql UPDATE employee SET SALARY=15000 where emp_name='jaggi'

# COMMAND ----------

# MAGIC %sql SELECT * FROM EMPLOYEE

# COMMAND ----------

# MAGIC %sql SET spark.databricks.delta.retentionDurationCheck.enabled = false
# MAGIC

# COMMAND ----------

# MAGIC %sql VACUUM EMPLOYEE RETAIN 0 hours

# COMMAND ----------

# MAGIC %md 
# MAGIC ### DEEP CLONING
# MAGIC ### SHALLOW  CLONING
# MAGIC ### CHANGE DATA FEED  CLONING
