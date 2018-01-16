package com.rama.bigdata

import org.apache.log4j._
import org.apache.spark.sql.{Row, SparkSession}


object TestShiftedBlockBuilding {

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder.master("local[*]").appName("example").getOrCreate()

    val df_cps1 = spark.
      read.
      option("header", "true").
      option("inferSchema", "true").
      csv("C:\\Users\\rlykhnenko\\Documents\\LenovoYoga\\Learnings\\" + "cps1.csv")

    df_cps1.show()
    println(df_cps1.count())

    // select columns of interest
    val df_Input = df_cps1.select("re74", "re75", "re78")

    val sumCount = df_cps1.select("re74").
      rdd.
      aggregate((0.0, 0))((acc, value1) => (acc._1 + value1.getDouble(0), acc._2 + 1),
        (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))

    println("sumCount" + sumCount)


    val zeroValue = new Array[Double](df_Input.columns.length)

    // convert Row to Array
    def rowToArr(inRow: Row): Array[Double] = {
      val result = new Array[Double](inRow.length)
      for (i <- 0 to (inRow.length - 1)) {
        result(i) = inRow.getDouble(i)
      }
      result
    }

    // calculate sum of two vectors element-wise, return result as an vector of the same length
    def sumOfArrays(ar1: Array[Double], ar2: Array[Double]): Array[Double] = {
      val sumVec = new Array[Double](ar1.length)
      for (i <- 0 to (ar1.length - 1)) {
        sumVec(i) = ar1(i) + ar2(i)
      }
      sumVec
    }

    def rowOperator(sum: Array[Double], row: Row): Array[Double] = { // do not change order of args
      sumOfArrays(sum, rowToArr(row))
    }

    val aggrNodes = df_Input.rdd.aggregate(zeroValue)(rowOperator(_, _), sumOfArrays(_, _))

    aggrNodes.map(println(_))

  }
}

//
//    val analysisID = args(0)
//    val urlStorage = args(1)
//    val storageName = args(2)
//    val spark = SparkSession.builder.getOrCreate()
//
//    // read input data
//    //    val df_BBS_big = spark.read.option("header", "true").option("inferSchema", "true").csv("wasb://atd-sql@" +
//    //      storageName + ".blob.core.windows.net/" + "BBS_big.csv")
//    //    df_BBS_big.show()
//
//    //    val cls = new ClassifierBBS(spark, df_BBS_big)
//    //    cls.apply
//
//
//    val df_cps1 = spark.
//      read.
//      option("header", "true").
//      option("inferSchema", "true").csv("wasb://exc4103-other@" + storageName + ".blob.core.windows.net/" + "cps1.csv")
//
//    df_cps1.show()
//
//    // select columns of interest
//    val df_Input = df_cps1.select("re74", "re75", "re78")
//
//    val zeroValue = new Array[Double](df_Input.columns.length)
//
//    // convert Row to Array
//    def rowToArr(inRow: Row): Array[Double] = {
//      val result = new Array[Double](inRow.length)
//
//      for (i <- 0 to (inRow.length - 1)) {
//        result(i) = inRow.getDouble(i)
//      }
//      result
//    }
//
//    // calculate sum of two vectors element-wise, return result as an vector of the same length
//    def sumOfArrays(ar1: Array[Double], ar2: Array[Double]): Array[Double] = {
//      val sumVec = new Array[Double](ar1.length)
//
//      for (i <- 0 to (ar1.length - 1)) {
//        sumVec(i) = ar1(i) + ar2(i)
//      }
//
//      sumVec
//    }
//
//
//    def rowOperator(sum: Array[Double], row: Row): Array[Double] = { // do not change order of args
//      sumOfArrays(sum, rowToArr(row))
//    }
//
//
//    val aggrNodes = df_Input.rdd.aggregate(zeroValue) ( rowOperator(_,_), sumOfArrays(_,_) )
//
//    aggrNodes.map(println(_))
//
//    //    val zeroValue = new Array[Double](df_Input.columns.length)
//    //
//    //    // convert Row to Array
//    //    def rowToArr(inRow: Row): Array[Double] = {
//    //      val result = new Array[Double](inRow.length)
//    //
//    //      for( i <-0  to (inRow.length - 1)){
//    //        result(i) = inRow.getDouble(i)
//    //      }
//    //      result
//    //    }
//    //
//    //    // fun 1
//    //    def sumOfArray(ar1:Array[Double], ar2:Array[Double]): Array[Double] ={
//    //      val sumVec = new Array[Double](ar1.length)
//    //
//    //      for( i <-0  to (ar1.length - 1)){
//    //        semVec(i) = ar1(i) + ar2(i)
//    //      }
//    //
//    //      sumVec
//    //    }
//    //
//    //    def rowOperator(sum:Array[Double], row: Row):Array[Double] ={
//    //      sumOfArray(sum,rowToArr(row) )
//    //    }
//    //
//    //
//    //
//    //    val aggrNodes = df_Input.rdd.aggregate(zeroValue) ( rowOperator(_,_), sumToArray(_,_) )
//
//  }
//
//}
