# Databricks notebook source
# MAGIC %sql 
# MAGIC MERGE INTO DELTA.Taxi_Zone_Delta as T1 
# MAGIC USING Taxi_Zone_Delta_view as S1
# MAGIC ON  T1.LocationID = S1.LocationID
# MAGIC WHEN MATCHED AND active_status='Y'
# MAGIC THEN UPDATE 
# MAGIC SET 
# MAGIC active_status='N' , end_date=current_date()
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (LocationID,
# MAGIC Borough,
# MAGIC Zone,
# MAGIC Service_zone,
# MAGIC active_status,
# MAGIC start_date,
# MAGIC end_date,
# MAGIC delete_date)
# MAGIC VALUES(
# MAGIC S1.LocationID,
# MAGIC S1.Borough,
# MAGIC S1.Zone,
# MAGIC S1.Service_zone,
# MAGIC 'Y',
# MAGIC current_date(),
# MAGIC '',
# MAGIC '')
