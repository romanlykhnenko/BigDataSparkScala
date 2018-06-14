package com.rama.bigdata

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

object FunctionFlow {

  case class schema(transactionId: String,
                    lineItemId: String)

  class schema2(var transactionId: String,
                var lineItemId: String) {

    def row: Row = Row.fromSeq(
      Seq(
        transactionId,
        lineItemId))
  }


  def main(args: Array[String]): Unit = {

    val spark: org.apache.spark.sql.SparkSession =
      org.apache.spark.sql.SparkSession.builder().getOrCreate()

    import spark.implicits._

    def transactionID(row: Row): String = row.getString(0)

    val someDF = Seq(
      ("a", "c"),
      ("a", "d"),
      ("a", "e"),

      ("b", "c"),
      ("b", "d"),
      ("b", "e"),

      ("c", "c"),
      ("c", "d"),
      ("c", "e")
    ).toDF("transactionId", "lineItemId")

    someDF.show()

    val schemaOutput = someDF.schema

    def f1(groupedRows: (String, Iterable[Row])): Array[Row] = {
      val key = groupedRows._1
      val rows = groupedRows._2
      val res = if (key == "a") rows.toArray else Array.empty[schema2].map(_.row)
      res
    }

    def f2(groupedRows: (String, Iterable[Row])): Array[Row] = {
      val key = groupedRows._1
      val rows = groupedRows._2
      val res = if (key == "b") rows.toArray else Array.empty[schema2].map(_.row)
      res
    }

    def f11(groupedRows: (String, Iterable[Row])): (String, Iterable[Row]) = {
      val key = groupedRows._1
      val rows = groupedRows._2
      val res = if (key == "a") rows.toArray else Array.empty[schema2].map(_.row)
      (key, res)
    }

    def f21(groupedRows: (String, Iterable[Row])): Array[Row] = {
      val key = groupedRows._1
      val rows = groupedRows._2
      val res = if (key == "a") rows.toArray else Array.empty[schema2].map(_.row)
      res
    }


    def fnctn(groupedRows: (String, Iterable[Row])): Array[Row] = {
      val resF1 = f11(groupedRows)
      f21(resF1)
    }

    println("Composition of function")
    val resRDD2 = someDF.
      rdd.
      groupBy(transactionID).
      flatMap(fnctn)


    //
    //
    //

    val resDF2 = spark.createDataFrame(resRDD2, StructType(schemaOutput))
    resDF2.show()

    println("Separate functions")
    val resRDD = someDF.
      rdd.
      groupBy(transactionID).
      flatMap(f1).
      groupBy(transactionID).
      flatMap(f2)

    val resDF = spark.createDataFrame(resRDD, StructType(schemaOutput))
    resDF.show()
  }
}
