# Databricks notebook source
# MAGIC %md
# MAGIC ### Zadanie 1
# MAGIC Od wersji 3.0 indeksowanie w Hive nie jest wspierane. Istnieją alternatywne metody które mogą zapewnić szybki dostęp do danych:
# MAGIC - użycie "materialized views": porównywane są stare i nowe tabele, zapytania są przepisywane w taki sposób aby optymalizować przetwarzanie danych kiedy następuje jakiś update w tabeli
# MAGIC - użycie kolumnowych formatów danych (Parquet, ORC) na których da się wykonać selective scanning

# COMMAND ----------

# MAGIC %md
# MAGIC ### Zadanie 3

# COMMAND ----------

spark.catalog.listDatabases()

# COMMAND ----------

spark.sql("CREATE DATABASE lab7")

# COMMAND ----------

filePath = "dbfs:/FileStore/tables/Files/names.csv"
df = spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .load(filePath)

df.write.mode("overwrite").saveAsTable("lab7.names")

# COMMAND ----------

filePath = "dbfs:/FileStore/tables/Files/actors.csv"
df = spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .load(filePath)

df.write.mode("overwrite").saveAsTable("lab7.actors")

# COMMAND ----------

spark.catalog.listTables("lab7")

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import *

# COMMAND ----------

def drop_tables(database):
    tables = [table.name for table in spark.catalog.listTables(database)]
    for t in tables:
        spark.sql(f"DELETE FROM {database}.{t}")
        print(f"Data from table {t} deleted.")

# COMMAND ----------

drop_tables("lab7")
