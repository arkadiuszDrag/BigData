// Databricks notebook source
// MAGIC %md 
// MAGIC Wykorzystaj dane z bazy 'bidevtestserver.database.windows.net'
// MAGIC ||
// MAGIC |--|
// MAGIC |SalesLT.Customer|
// MAGIC |SalesLT.ProductModel|
// MAGIC |SalesLT.vProductModelCatalogDescription|
// MAGIC |SalesLT.ProductDescription|
// MAGIC |SalesLT.Product|
// MAGIC |SalesLT.ProductModelProductDescription|
// MAGIC |SalesLT.vProductAndDescription|
// MAGIC |SalesLT.ProductCategory|
// MAGIC |SalesLT.vGetAllCategories|
// MAGIC |SalesLT.Address|
// MAGIC |SalesLT.CustomerAddress|
// MAGIC |SalesLT.SalesOrderDetail|
// MAGIC |SalesLT.SalesOrderHeader|

// COMMAND ----------


val jdbcHostname = "bidevtestserver.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "testdb"

val tabela = spark.read
  .format("jdbc")
  .option("url", s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user", "sqladmin")
  .option("password", "$3bFHs56&o123$")
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query", "SELECT * FROM INFORMATION_SCHEMA.TABLES")
  .load()


display(tabela)

// COMMAND ----------

// MAGIC %md
// MAGIC 1. Pobierz wszystkie tabele z schematu SalesLt i zapisz lokalnie bez modyfikacji w formacie delta

// COMMAND ----------

val SalesLT = tabela.where("TABLE_SCHEMA == 'SalesLT'")

val names = SalesLT.select("TABLE_NAME").as[String].collect.toList
print(names)

for( i <- names ){
  val tab = spark.read
  .format("jdbc")
  .option("url", s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user", "sqladmin")
  .option("password", "$3bFHs56&o123$")
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query", s"SELECT * FROM SalesLT.$i")
  .load()
  
  tab.write.format("delta").mode("overwrite").saveAsTable(i)  
}

// COMMAND ----------

// MAGIC %md
// MAGIC  Uzycie Nulls, fill, drop, replace, i agg
// MAGIC  * W każdej z tabel sprawdź ile jest nulls w rzędach i kolumnach
// MAGIC  * Użyj funkcji fill żeby dodać wartości nie występujące w kolumnach dla wszystkich tabel z null
// MAGIC  * Użyj funkcji drop żeby usunąć nulle, 
// MAGIC  * wybierz 3 dowolne funkcje agregujące i policz dla TaxAmt, Freight, [SalesLT].[SalesOrderHeader]
// MAGIC  * Użyj tabeli [SalesLT].[Product] i pogrupuj według ProductModelId, Color i ProductCategoryID i wylicz 3 wybrane funkcje agg() 
// MAGIC    - Użyj conajmniej dwóch overloded funkcji agregujących np z (Map)

// COMMAND ----------

// MAGIC %fs ls /user/hive/warehouse

// COMMAND ----------

import org.apache.spark.sql.functions.{col, when, count}
import org.apache.spark.sql.Column


def countCols(columns: Array[String]): Array[Column]={
    columns.map(c=>{
      count(when(col(c).isNull,c)).alias(c)
    })
}

// COMMAND ----------

val names = SalesLT.select("TABLE_NAME").as[String].collect.toList

// COMMAND ----------

// W każdej z tabel sprawdź ile jest nulls w rzędach i kolumnach
//Ilosc nulli w kolumnach

for( i <- names ){
  val tab = spark.read.format("delta").option("header", "true").load("dbfs:/user/hive/warehouse/"+i.toLowerCase())
  tab.select(countCols(tab.columns):_*).show()
}

// COMMAND ----------


for( i <- names ){
  val tab = spark.read.format("delta").option("header", "true").load("dbfs:/user/hive/warehouse/"+i.toLowerCase())
  val tab_filled = tab.na.fill("999", tab.columns)
  display(tab_filled)
}

// COMMAND ----------


for( i <- names ){
  val tab = spark.read.format("delta").option("header", "true").load("dbfs:/user/hive/warehouse/"+i.toLowerCase())
  val tab_na_dropped = tab.na.drop().show(false)
}

// COMMAND ----------

import org.apache.spark.sql.functions._

val sales_order_header = spark.read.format("delta").option("header", "true").load("dbfs:/user/hive/warehouse/salesorderheader")

sales_order_header.select(stddev("TaxAmt").as("std_TaxAmt"), mean("TaxAmt").as("mean_TaxAmt"), sumDistinct("TaxAmt").as("sum_distinct_TaxAmt"), min("Freight").as("min_Freight"), max("Freight").as("max_Freight"), corr("TaxAmt", "Freight").as("corr_TaxAmt_Freight")).show()


// COMMAND ----------

val tab_product = spark.read.format("delta").option("header", "true").load("dbfs:/user/hive/warehouse/product")
val groupped_tab_product = tab_product.groupBy("ProductModelId", "Color", "ProductCategoryId").count()

groupped_tab_product.select(sumDistinct("ProductModelId"), ).show()

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/names.csv"
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)


display(namesDf)

// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

val upperUDF = udf { s: String => s.toUpperCase }

namesDf.withColumn("upper", upperUDF('bio)).show

// COMMAND ----------

val doubleUDF = udf { d: Double => d.toDouble }

namesDf.withColumn("heightDouble", doubleUDF('height)).show

// COMMAND ----------

val integerUDF = udf { i: Integer => i.asInstanceOf[Integer] }

namesDf.withColumn("heightDouble", doubleUDF('height)).withColumn("heightInt", integerUDF('heightDouble)).show
