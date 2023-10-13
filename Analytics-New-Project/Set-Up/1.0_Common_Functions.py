# Databricks notebook source
from pyspark.sql.functions import current_timestamp,input_file_name
def add_date_file_name(input_df):
    output_df=input_df.withColumn("Ingestion_Date" , current_timestamp()) \
                     .withColumn("File_Path" , input_file_name())
    return output_df
