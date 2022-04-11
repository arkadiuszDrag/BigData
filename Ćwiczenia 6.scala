// Databricks notebook source
spark.sql("create database Sample")

// COMMAND ----------

import org.apache.spark.sql.types.{IntegerType,StringType,StructType,StructField}
import org.apache.spark.sql.{Row, SparkSession}

val transactionsData = Seq(Row( 1, "2011-01-01", 500),
  Row( 1, "2011-01-15", 50),
  Row( 1, "2011-01-22", 250),
  Row( 1, "2011-01-24", 75),
  Row( 1, "2011-01-26", 125),
  Row( 1, "2011-01-28", 175),
  Row( 2, "2011-01-01", 500),
  Row( 2, "2011-01-15", 50),
  Row( 2, "2011-01-22", 25),
  Row( 2, "2011-01-23", 125),
  Row( 2, "2011-01-26", 200),
  Row( 2, "2011-01-29", 250),
  Row( 3, "2011-01-01", 500),
  Row( 3, "2011-01-15", 50 ),
  Row( 3, "2011-01-22", 5000),
  Row( 3, "2011-01-25", 550),
  Row( 3, "2011-01-27", 95 ),
  Row( 3, "2011-01-30", 2500) )


// COMMAND ----------

val logicalData = Seq(Row(1,"George", 800),
  Row(2,"Sam", 950),
  Row(3,"Diane", 1100),
  Row(4,"Nicholas", 1250),
  Row(5,"Samuel", 1250),
  Row(6,"Patricia", 1300),
  Row(7,"Brian", 1500),
  Row(8,"Thomas", 1600),
  Row(9,"Fran", 2450),
  Row(10,"Debbie", 2850),
  Row(11,"Mark", 2975),
  Row(12,"James", 3000),
  Row(13,"Cynthia", 3000),
  Row(14,"Christopher", 5000) )

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


val windowFun  = Window.partitionBy(col("AccountId")).orderBy("TranDate")
val run_total=sum("TranAmt").over(windowFun)

transactionsDf.select(col("AccountId"), col("TranDate"), col("TranAmt") ,run_total.alias("RunTotalAmt") ).orderBy("AccountId").show()

// COMMAND ----------

val windowFunRange  = windowFun.rowsBetween(-2, Window.currentRow) 

val transactionDfWindowsRange = transactionsDf.withColumn("SlideTotal",run_total)
.withColumn("SlideAvg",avg("TranAmt").over(windowFun))
.withColumn("SlideMin",min("TranAmt").over(windowFun))
.withColumn("SlideMax",max("TranAmt").over(windowFun))
.withColumn("SlideQty",count("*").over(windowFun))
.withColumn("RN",row_number().over(windowFun))
display(transactionDfWindows)

// COMMAND ----------

val windowFunRows  = Window.orderBy("Salary").rowsBetween(Window.unboundedPreceding, Window.currentRow) 
val  windowSpecRange  = Window.orderBy("Salary").rangeBetween(Window.unboundedPreceding, Window.currentRow) 

val logicalDfWindows = logicalDf.withColumn("SumByRows",sum(col("Salary")).over(windowFunRows)).withColumn("SumByRange",sum(col("Salary")).over(windowSpecRange))
display(logicalDfWindows)


// COMMAND ----------

val tab = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(s"dbfs:/user/hive/warehouse/salesorderheader")

val windowFun  = Window.partitionBy("AccountNumber").orderBy("OrderDate")
val tabWindow=tab.select(col("AccountNumber"), col("OrderDate"), col("TotalDue") ,row_number().over(windowFun).alias("RN") ).orderBy("AccountNumber").limit(10)
display(tabWindow)

// COMMAND ----------

display(tab)

// COMMAND ----------

val windowFun =Window.partitionBy(col("Status")).orderBy("OrderDate")
val windowFunRows=windowFun.rowsBetween(Window.unboundedPreceding, -2) 


val tabRows = tab.withColumn("lead",lead(col("SubTotal"), 2).over(windowFun))
.withColumn("lag",lag(col("SubTotal"),1).over(windowFun))
.withColumn("last",last(col("SubTotal")).over(windowFunRows))
.withColumn("first",first(col("SubTotal")).over(windowFunRows))
.withColumn("RN",row_number().over(windowFun))
.withColumn("DR",dense_rank().over(windowFun))

// COMMAND ----------

display(tabRows)

// COMMAND ----------

val tabDetails = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(s"dbfs:/user/hive/warehouse/salesorderdetail")


val joinLeft = tab.join(tabDetails, tab.col("SalesOrderID") === tabDetails.col("SalesOrderID"), "leftsemi")
joinLeft.show()

// COMMAND ----------

joinLeft.explain()

// COMMAND ----------

val joinLeftAnti = tab.join(tabDetails, tab.col("SalesOrderID") === tabDetails.col("SalesOrderID"), "leftanti")
joinLeftAnti.show()

// COMMAND ----------

joinLeftAnti.explain()

// COMMAND ----------

display(joinLeft)

// COMMAND ----------

display(joinLeft.distinct())

// COMMAND ----------

display(joinLeft.dropDuplicates())

// COMMAND ----------

import org.apache.spark.sql.functions.broadcast
val joinBroadcast = tab.join(broadcast(tabDetails), tab.col("SalesOrderID") === tabDetails.col("SalesOrderID"))
joinBroadcast.show()

// COMMAND ----------

joinBroadcast.explain()
