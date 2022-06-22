// Databricks notebook source
import agh.wggios.analizadanych._

// COMMAND ----------

display(dbutils.fs.ls("FileStore/tables/flight_data.csv"))

// COMMAND ----------

val args: Array[String] =  Array("dbfs:/FileStore/tables/flight_data.csv")
Main.main(args)

// COMMAND ----------

// MAGIC %md
// MAGIC partitionBy and bucketBy

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/movies.csv"
val df = spark.read.format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load(filePath)
val newDf= df.write.format("csv").bucketBy(4,"title")
    .mode("overwrite")
    .saveAsTable("bucketed_table")

// COMMAND ----------

val df2=spark.sql("select * from bucketed_table limit 5")
display(df2)

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/"))

// COMMAND ----------

spark.table("bucketed_table").rdd.getNumPartitions

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/user/hive/warehouse/bucketed_table/"))

// COMMAND ----------

val newDf2= df.write.format("csv").partitionBy("title")
    .mode("overwrite")
    .saveAsTable("partition_table")

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/user/hive/warehouse/partition_table/"))

// COMMAND ----------

// MAGIC %md
// MAGIC partition by stworzyl o wiele wiecej plikow 

// COMMAND ----------

// MAGIC %sql 
// MAGIC ANALYZE TABLE bucketed_table COMPUTE STATISTICS FOR ALL COLUMNS;
