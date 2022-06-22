// Databricks notebook source
val path = "dbfs:/FileStore/tables/Nested.json"
val jsonDF = spark.read.option("multiline","true").json(path)
display(jsonDF)

// COMMAND ----------

display(jsonDF.withColumn("pathLinkInfo_edit", $"pathLinkInfo".dropFields("alternateName", "elevationGain")))

// COMMAND ----------

display(jsonDF.withColumn("elevationGain", $"pathLinkInfo".dropFields("alternateName", "captureSpecification", "cycleFacility", "endGradeSeparation", "endNode", "fictitious", "formOfWay", "formsPartOfPath", "heightingMethod", "matchStatus", "pathName", "startGradeSeparation", "startNode", "surfaceType", "formsPartOfStreet", "sourceFID")).withColumn("elevationAgainstDirection",$"elevationGain".dropFields("elevationGain.elevationInDirection")))

// COMMAND ----------

val inputList: List[Int] = List(1, 3, 5)
inputList.foldLeft(0) { (acc, i) => acc + i }

// COMMAND ----------

// The foldLeft equivalent of List.sum.
def sum(list: List[Int]): Int = list.foldLeft(0)(_ +_)
sum(inputList)

// COMMAND ----------

// The foldLeft equivalent of List.length.
def len(list: List[Any]): Int = list.foldLeft(0) { (count, _) => count + 1 }
len(inputList)

// COMMAND ----------

//The foldLeft equivalent of List.last
def last[A](list: List[A]): A = list.foldLeft[A](list.head) { (_, cur) => cur }
last(inputList)

// COMMAND ----------

// Calculate the average of values from a given List[Double]
def average(list: List[Double]): Double = list match {
  case head :: tail => tail.foldLeft((head, 1.0)) { (avg, cur) =>
    ((avg._1 * avg._2 + cur)/(avg._2 + 1.0), avg._2 + 1.0)
  }._1
  case Nil => -1
}
average( List(1.0, 3.9, 5.9))

// COMMAND ----------

//Reverse the order of a list.
def reverse[A](list: List[A]): List[A] =
  list.foldLeft(List[A]()) { (r,c) => c :: r }
reverse(inputList)

// COMMAND ----------

val excludedNestedFields = Map("pathLinkInfo" -> Array("alternateName","captureSpecification"),"NewColumn" -> Array("heightingMethod","pathName"))
display(excludedNestedFields.foldLeft(jsonDF){(k,v) =>(
k.withColumn("NewColumn",col(v._1).dropFields(v._2:_*))
)})
