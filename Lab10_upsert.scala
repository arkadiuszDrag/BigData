// Databricks notebook source
// MAGIC %md
// MAGIC ## Dane 
// MAGIC 
// MAGIC * Użyj danych do zadania '../retail-data/all/online-retail-dataset.csv'

// COMMAND ----------

// MAGIC %md
// MAGIC Set up relevant paths.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## UPSERT 
// MAGIC 
// MAGIC Literally means "UPdate" and "inSERT". It means to atomically either insert a row, or, if the row already exists, UPDATE the row.
// MAGIC 
// MAGIC Alter data by changing the values in one of the columns for a specific `CustomerID`.
// MAGIC 
// MAGIC Let's load the CSV file `../outdoor-products-mini.csv`.

// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField, DoubleType, IntegerType, StringType}

lazy val inputSchema = StructType(List(
  StructField("InvoiceNo", IntegerType, true),
  StructField("StockCode", StringType, true),
  StructField("Description", StringType, true),
  StructField("Quantity", IntegerType, true),
  StructField("InvoiceDate", StringType, true),
  StructField("UnitPrice", DoubleType, true),
  StructField("CustomerID", IntegerType, true),
  StructField("Country", StringType, true)
))


// COMMAND ----------

val data = "https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/retail-data/all/online-retail-dataset.csv"
spark.sparkContext.addFile(data)

val miniDataDF = spark.read
      .option("header", true)
      .schema(inputSchema)
      .csv("file://" + org.apache.spark.SparkFiles.get("online-retail-dataset.csv"))
display(miniDataDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## UPSERT Using Non-Databricks Delta Pipeline
// MAGIC 
// MAGIC This feature is not supported in non-Delta pipelines.
// MAGIC 
// MAGIC To UPSERT means to "UPdate" and "inSERT". In other words, UPSERT is not an atomic operation. It is literally TWO operations. 
// MAGIC 
// MAGIC Running an UPDATE could invalidate data that is accessed by the subsequent INSERT operation.

// COMMAND ----------

// MAGIC %md
// MAGIC ## UPSERT Using Databricks Delta Pipeline
// MAGIC 
// MAGIC Using Databricks Delta, however, we can do UPSERTS.

// COMMAND ----------

(miniDataDF
  .write
  .mode("overwrite")
  .format("delta")
  .save("dbfs:/tmp/delta/upsert/online-retail-dataset") 
)

spark.sql(s"""
    DROP TABLE IF EXISTS customer_data_delta_mini
  """)
spark.sql(s"""
    CREATE TABLE customer_data_delta_mini
    USING DELTA 
    LOCATION "dbfs:/tmp/delta/upsert/online-retail-dataset"
  """)

// COMMAND ----------

// MAGIC %md
// MAGIC List all rows with `CustomerID=20993`.

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM customer_data_delta_mini WHERE CustomerID=20993

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC Form a new DataFrame where `StockCode` is `99999` for `CustomerID=20993`.
// MAGIC 
// MAGIC Create a table `customer_data_delta_to_upsert` that contains this data.
// MAGIC 
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** You need to convert `InvoiceNo` to a `String` because Delta infers types and `InvoiceNo` looks like it should be an integer.

// COMMAND ----------

import org.apache.spark.sql.functions.lit
val customerSpecificDF = (miniDataDF
  //.filter("CustomerID=20993")
  .withColumn("StockCode", lit(99999))
  .withColumn("InvoiceNo", $"InvoiceNo".cast("String"))
 )

spark.sql("DROP TABLE IF EXISTS customer_data_delta_to_upsert")
customerSpecificDF.write.saveAsTable("customer_data_delta_to_upsert")

// COMMAND ----------

// MAGIC %md
// MAGIC Upsert the new data into `customer_data_delta_mini`.
// MAGIC 
// MAGIC Upsert is done using the `MERGE INTO` syntax.

// COMMAND ----------

// MAGIC %sql select count(*) from customer_data_delta_mini

// COMMAND ----------

// MAGIC %sql
// MAGIC MERGE INTO customer_data_delta_mini
// MAGIC USING customer_data_delta_to_upsert
// MAGIC ON customer_data_delta_mini.CustomerID = customer_data_delta_to_upsert.CustomerID
// MAGIC WHEN MATCHED THEN
// MAGIC   UPDATE SET
// MAGIC     customer_data_delta_mini.StockCode = customer_data_delta_to_upsert.StockCode
// MAGIC WHEN NOT MATCHED
// MAGIC   THEN INSERT (InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country)
// MAGIC   VALUES (
// MAGIC     customer_data_delta_to_upsert.InvoiceNo,
// MAGIC     customer_data_delta_to_upsert.StockCode, 
// MAGIC     customer_data_delta_to_upsert.Description, 
// MAGIC     customer_data_delta_to_upsert.Quantity, 
// MAGIC     customer_data_delta_to_upsert.InvoiceDate, 
// MAGIC     customer_data_delta_to_upsert.UnitPrice, 
// MAGIC     customer_data_delta_to_upsert.CustomerID, 
// MAGIC     customer_data_delta_to_upsert.Country)

// COMMAND ----------

// MAGIC %md
// MAGIC Notice how this data is seamlessly incorporated into `customer_data_delta_mini`.

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM customer_data_delta_mini WHERE CustomerID=20993

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Exercise 1
// MAGIC 
// MAGIC Create a DataFrame out of the table `demo_iot_data_delta`.

// COMMAND ----------

// TODO
val DF = spark.sql("select * from customer_data_delta_mini")

// COMMAND ----------

// TEST - Run this cell to test your solution.
import org.apache.spark.sql.types.{StructType, StructField, StringType, LongType, DateType, IntegerType}

lazy val expectedSchema = StructType(
  
  List(
   StructField("action", StringType, true),
   StructField("time", LongType, true),
   StructField("date", DateType, true),
   StructField("deviceId", IntegerType, true)
))
// Porównaj schematy
DF.printSchema


// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Exercise 2
// MAGIC 
// MAGIC Create another dataframe where you change`action` to `Close` for `date = '2018-06-01' ` and `deviceId = 485`.
// MAGIC 
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Use `distinct`.
// MAGIC 
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Consider using `selectExpr()`.

// COMMAND ----------

// TODO
val DeviceId485DF = newDataDF
  .selectExpr(" 'Close' as Action", "time" ,"date" ,"deviceId")
  .distinct()
  .filter("date = '2018-06-01' ")
  .filter("deviceId = 485")

// COMMAND ----------

newDeviceId485DF.explain(true)

// COMMAND ----------

// TEST - Run this cell to test your solution.
 val actionCount = DeviceId485DF.select("Action").count
//display(actionCount)
dbTest("Delta-L4-actionCount", 1, actionCount)

println("Tests passed!")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Exercise 3
// MAGIC 
// MAGIC Write to a new Databricks Delta table that contains just our data to be upserted.
// MAGIC 
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** You can adapt the SQL syntax for the upsert from our demo example, above.

// COMMAND ----------

// TODO
spark.sql("DROP TABLE IF EXISTS iot_data_delta_to_upsert")
DeviceId485DF.write.saveAsTable("iot_data_delta_to_upsert")

// COMMAND ----------

// TEST - Run this cell to test your solution.
lazy val tableExists = spark.catalog.tableExists("demo_iot_data_delta")
lazy val count = spark.table("iot_data_delta_to_upsert").count()



// COMMAND ----------

// MAGIC %sql
// MAGIC --TODO
// MAGIC MERGE INTO demo_iot_data_delta
// MAGIC USING iot_data_delta_to_upsert
// MAGIC ON demo_iot_data_delta.deviceId = iot_data_delta_to_upsert.deviceId
// MAGIC WHEN MATCHED THEN
// MAGIC   UPDATE SET
// MAGIC     demo_iot_data_delta.action = iot_data_delta_to_upsert.action
// MAGIC WHEN NOT MATCHED
// MAGIC   THEN INSERT (action, time, date, deviceId)
// MAGIC   VALUES (
// MAGIC     iot_data_delta_to_upsert.action, 
// MAGIC     iot_data_delta_to_upsert.time, 
// MAGIC     iot_data_delta_to_upsert.date, 
// MAGIC     iot_data_delta_to_upsert.deviceId 
// MAGIC )

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 4
// MAGIC 
// MAGIC Count the number of items in `demo_iot_data_delta` where the `deviceId` is `485` and `action` is `Close`.

// COMMAND ----------

// TODO
val count = spark.sql("SELECT count(*) as total FROM demo_iot_data_delta WHERE deviceId = 485 AND action = 'Close' ").collect()(0)(0)
