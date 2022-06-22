// Databricks notebook source
// MAGIC %md
// MAGIC 1. Pobierz dane Spark-The-Definitive_Guide dostępne na github
// MAGIC 2. Użyj danych do zadania '../retail-data/all/online-retail-dataset.csv'

// COMMAND ----------

// MAGIC %md
// MAGIC **POBRANIE DANYCH**

// COMMAND ----------

import org.apache.spark.SparkFiles

// COMMAND ----------

val data = "https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/retail-data/all/online-retail-dataset.csv"
spark.sparkContext.addFile(data)

val df = spark.read
      .option("inferSchema", true)
      .option("header", true)
      .option("overwriteSchema", "true")
      .csv("file:///" + SparkFiles.get("online-retail-dataset.csv"))
df.show(3)

// COMMAND ----------

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC 3. Zapisz DataFrame do formatu delta i stwórz dużą ilość parycji (kilkaset)
// MAGIC * Partycjonuj po Country

// COMMAND ----------

// MAGIC %md 
// MAGIC **ZAPIS DO FORMATU DELTA**

// COMMAND ----------

val df2=df.repartition(30)

// COMMAND ----------

df2.write.format("delta").partitionBy("Country").mode("overwrite").save("/tmp/delta/online_retail_dataset")

// COMMAND ----------

display(dbutils.fs.ls("/tmp/delta/online_retail_dataset/"))

// COMMAND ----------

spark.sql("DROP TABLE IF EXISTS online_retail_dataset")

spark.sql(s"""
  CREATE TABLE online_retail_dataset
  USING Delta
  LOCATION "dbfs:/tmp/delta/online_retail_dataset"
""")

// COMMAND ----------

spark.sql("show tables from default").show()

// COMMAND ----------

val p = "dbfs:/tmp/delta/online_retail_dataset"
val dfDelta = spark.read
  .format("delta").load(p)
dfDelta.rdd.getNumPartitions

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1: OPTIMIZE and ZORDER
// MAGIC 
// MAGIC Wykonaj optymalizację do danych stworzonych w części I `../delta/retail-data/`.
// MAGIC 
// MAGIC Dane są partycjonowane po kolumnie `Country`.
// MAGIC 
// MAGIC Przykładowe zapytanie dotyczy `StockCode`  = `22301`. 
// MAGIC 
// MAGIC Wykonaj zapytanie i sprawdź czas wykonania. Działa szybko czy wolno 
// MAGIC 
// MAGIC Zmierz czas zapytania kod poniżej - przekaż df do `sqlZorderQuery`.

// COMMAND ----------

display(spark.sql("select * from online_retail_dataset where StockCode = '22301'"))

// COMMAND ----------

// TODO
def timeIt[T](op: => T): Float = {
 val start = System.currentTimeMillis
 val res = op
 val end = System.currentTimeMillis
 (end - start) / 1000.toFloat
}

val sqlZorderQuery = timeIt(spark.sql("select * from online_retail_dataset where StockCode = '22301'").collect())//ok. 66 sek

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from deltaRetailData where StockCode = '22301'

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Skompaktuj pliki i przesortuj po `StockCode`.

// COMMAND ----------

// MAGIC %sql
// MAGIC -- wypelnij
// MAGIC OPTIMIZE online_retail_dataset
// MAGIC ZORDER by (StockCode)

// COMMAND ----------

// MAGIC %md
// MAGIC Uruchom zapytanie ponownie tym razem użyj `postZorderQuery`.

// COMMAND ----------

// TODO
val poZorderQuery = timeIt(spark.sql("select * from online_retail_dataset where StockCode = '22301'").collect())//ok. 4 s

// COMMAND ----------

// MAGIC %md
// MAGIC ## 2: VACUUM
// MAGIC 
// MAGIC Policz liczbę plików przed wykonaniem `VACUUM` for `Country=Sweden` lub innego kraju

// COMMAND ----------

// TODO
val plikiPrzed = dbutils.fs.ls("dbfs:/tmp/delta/online_retail_dataset/Country=Sweden").length//31 plikow

// COMMAND ----------

// MAGIC %md
// MAGIC Teraz wykonaj `VACUUM` i sprawdź ile było plików przed i po.

// COMMAND ----------

// MAGIC %sql
// MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false;
// MAGIC VACUUM online_retail_dataset RETAIN 0 HOURS;

// COMMAND ----------

// MAGIC %md
// MAGIC Policz pliki dla wybranego kraju `Country=Sweden`.

// COMMAND ----------

// TODO
val plikiPo = dbutils.fs.ls("dbfs:/tmp/delta/online_retail_dataset/Country=Sweden").length//1 plik

// COMMAND ----------

// MAGIC %md
// MAGIC ## Przeglądanie histrycznych wartośći
// MAGIC 
// MAGIC możesz użyć funkcji `describe history` żeby zobaczyć jak wyglądały zmiany w tabeli. Jeśli masz nową tabelę to nie będzie w niej history, dodaj więc trochę danych żeby zoaczyć czy rzeczywiście się zmieniają. 

// COMMAND ----------

// MAGIC %sql
// MAGIC describe history online_retail_dataset
