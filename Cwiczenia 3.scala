// Databricks notebook source
// MAGIC %md Names.csv 
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę w której wyliczysz wzrost w stopach (feet)
// MAGIC * Odpowiedz na pytanie jakie jest najpopularniesze imię?
// MAGIC * Dodaj kolumnę i policz wiek aktorów 
// MAGIC * Usuń kolumny (bio, death_details)
// MAGIC * Zmień nazwy kolumn - dodaj kapitalizaję i usuń _
// MAGIC * Posortuj dataframe po imieniu rosnąco

// COMMAND ----------


val filePath = "dbfs:/FileStore/tables/Files/names.csv"
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)


display(namesDf)

// COMMAND ----------

val newDf = namesDf.withColumn("epoch", from_unixtime(unix_timestamp()))
display(newDf)

// COMMAND ----------

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType



val namesDf_1 = namesDf.withColumn("heightFeets", col("height").cast(DoubleType)*0.0328)

namesDf_1.select("heightFeets").show()

// COMMAND ----------

import  org.apache.spark.sql.functions._
import spark.sqlContext.implicits._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

import sqlContext.implicits._
import org.apache.spark.sql._

val namesDf_2 = namesDf_1.select(split(col("name")," ").as("NameArray")).toDF("Names")

val eachNameCount = namesDf_2.groupBy(col("Names")(0)).count()

println("The most common name is ", eachNameCount.sort(desc("count")).head())


// COMMAND ----------

import org.apache.spark.sql.functions.{current_date, current_timestamp, date_format}

val formatedDates_birth = namesDf_1.select(col("imdb_name_id"), col("date_of_birth"), 
                 when(to_date(col("date_of_birth"), "dd.MM.yyyy").isNotNull,
                                            to_date(col("date_of_birth"), "dd.MM.yyyy"))
                 .when(to_date(col("date_of_birth"), "yyyy-MM-dd").isNotNull, 
                                            to_date(col("date_of_birth"), "yyyy-MM-dd")).as("Formated Date")).toDF().show()




// COMMAND ----------

val actors_age = feet.withColumn("simple age", floor(datediff(current_date(), $"date_of_birth")/365))

// COMMAND ----------

import org.apache.spark.sql.catalyst.plans.Inner

val formatedDates_death = namesDf_1.select(col("imdb_name_id"), col("date_of_death"), to_date(col("date_of_death"), "dd.MM.yyyy").as("Formated Date")).toDF()

val a = formatedDates_birth.join(formatedDates_death, Seq("imdb_name_id"))


// COMMAND ----------

val dat = namesDf_1.agg(when(col("date_of_death"), to_date(col("date_of_death"), "dd.MM.yyyy")).isNotNull.count()
         
val dat1 = namesDf_1.select(col("date_of_death")).count()

// COMMAND ----------

val namesDf_3 = namesDf_1.drop("bio", "death_details")
namesDf_3.show()

// COMMAND ----------

import spark.sqlContext.implicits._


// COMMAND ----------

val old_columns = Seq("imdb_name_id","name","birth_name","height","birth_details","date_of_birth","place_of_birth","date_of_death","place_of_death","reason_of_death","spouses_string","spouses","divorces","spouses_with_children","children","heightFeets")
val new_columns = Seq("imdbNameId","name","birthName","height","birthDetails","dateOfBirth","placeOfBirth","dateOfDeath","placeOfDeath","reasonOfDeath","spousesSring","spouses","divorces","spousesWithChildren","children","heightFeets")

val columnsList = old_columns.zip(new_columns).map(f=>{col(f._1).as(f._2)})

val namesDf_4 = namesDf_3.select(columnsList:_*)
namesDf_4.printSchema()

// COMMAND ----------

namesDf_4.sort(asc("name")).show()

// COMMAND ----------

// MAGIC %md Movies.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę która wylicza ile lat upłynęło od publikacji filmu
// MAGIC * Dodaj kolumnę która pokaże budżet filmu jako wartość numeryczną, (trzeba usunac znaki walut)
// MAGIC * Usuń wiersze z dataframe gdzie wartości są null

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/movies.csv"
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(namesDf)

// COMMAND ----------

val df = moviesDf.withColumn("epoch", from_unixtime(unix_timestamp()))

// COMMAND ----------

val df1 = namesDf.withColumn("diff", expr("2022 - year")).select("diff").show()

// COMMAND ----------

val df2 = namesDf.select(split(col("budget")," ").as("budgetArray")).select(col("budgetArray")(1).as("numBudget")).show()
//val df3 = df2.drop("budgetArray")

// COMMAND ----------

val df4 = namesDf.na.drop().show(false)

// COMMAND ----------

// MAGIC %md ratings.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dla każdego z poniższych wyliczeń nie bierz pod uwagę `nulls` 
// MAGIC * Dodaj nowe kolumny i policz mean i median dla wartości głosów (1 d 10)
// MAGIC * Dla każdej wartości mean i median policz jaka jest różnica między weighted_average_vote
// MAGIC * Kto daje lepsze oceny chłopcy czy dziewczyny dla całego setu
// MAGIC * Dla jednej z kolumn zmień typ danych do `long` 

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/ratings.csv"
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(namesDf)

// COMMAND ----------

val selected_cols_names = df.columns.toList.slice(5,15)

val mean_votes_cols = df.select(selected_cols_names.map(c => col(c)): _*)

val mean_votes = df.withColumn("mean_votes[1-10]", mean_votes_cols.columns.map(c => col(c)).reduce((c1, c2) => c1 + c2) / lit(mean_votes_cols.columns.length))

// COMMAND ----------

val ratingsAvg = mean_votes.select("females_allages_avg_vote","males_allages_avg_vote").agg(avg("females_allages_avg_vote") as "female_votes_avg", avg("males_allages_avg_vote") as "male_votes_avg")

// COMMAND ----------

val median_votes = df.withColumn("median_votes", )

// COMMAND ----------

import org.apache.spark.sql.types.{LongType}

val cast_type = df.withColumn("weighted_avg_vote_long ", col("weighted_average_vote").cast(LongType))

cast_type.printSchema


// COMMAND ----------

val select_explain = cast_type.select("imdb_title_id", "total_votes").explain()
val groupby_explain = cast_type.select("imdb_title_id", "total_votes").groupBy("imdb_title_id").count().explain()

