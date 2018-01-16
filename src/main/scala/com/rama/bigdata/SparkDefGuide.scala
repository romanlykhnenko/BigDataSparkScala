package com.rama.bigdata

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

case class Flight(DEST_COUNTRY_NAME: String,
                  ORIGIN_COUNTRY_NAME: String, count: BigInt)

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.util.AccumulatorV2

// Custom Accumulators
class EvenAccumulator extends AccumulatorV2[BigInt, BigInt] {
  private var num:BigInt = 0
  def reset(): Unit = {
    this.num = 0
  }
  def add(intValue: BigInt): Unit = {
    if (intValue % 2 == 0) {
      this.num += intValue
    }
  }
  def merge(other: AccumulatorV2[BigInt,BigInt]): Unit = {
    this.num += other.value
  }
  def value():BigInt = {
    this.num
  }
  def copy(): AccumulatorV2[BigInt,BigInt] = {
    new EvenAccumulator
  }
  def isZero():Boolean = {
    this.num == 0
  }
}



object SparkDefGuide {
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.
      builder.
      master("local[*]").
      appName("example").
      getOrCreate()

    //
    // Chapter 14. Distributed Shared Variables
    //

//    // Broadcast Variables
//    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
//      .split(" ")
//    val words = spark.sparkContext.parallelize(myCollection, 2)
//
//    val supplementalData = Map("Spark" -> 1000, "Definitive" -> 200,
//      "Big" -> -300, "Simple" -> 100)
//
//    val suppBroadcast = spark.sparkContext.broadcast(supplementalData)
//
//    //suppBroadcast.value
//
//    words.map(word => (word, suppBroadcast.value.getOrElse(word, 0)))
//      .sortBy(wordPair => wordPair._2)
//      .collect()
//      .foreach(println(_))

    // Accumulators

    import spark.implicits._

    val flights = spark.read
      .parquet("C:\\Users\\rlykhnenko\\Documents\\LenovoYoga\\allGitReposPr\\Spark-The-Definitive-Guide\\data\\flight-data\\parquet\\2010-summary.parquet")
      .as[Flight]


    val accUnnamed = new LongAccumulator
    val acc = spark.sparkContext.register(accUnnamed)

    val accChina = new LongAccumulator
    val accChina2 = spark.sparkContext.longAccumulator("China")
    spark.sparkContext.register(accChina, "China")

    def accChinaFunc(flight_row: Flight) = {
      val destination = flight_row.DEST_COUNTRY_NAME
      val origin = flight_row.ORIGIN_COUNTRY_NAME
      if (destination == "China") {
        accChina.add(flight_row.count.toLong)
      }
      if (origin == "China") {
        accChina.add(flight_row.count.toLong)
      }
    }

    flights.foreach(flight_row => accChinaFunc(flight_row))
    println("Accumulator = " + accChina.value)



    // Custom accumulators

    val arr = ArrayBuffer[BigInt]()
    val acc3 = new EvenAccumulator()
    val newAcc = spark.sparkContext.register(acc3, "evenAcc")

    acc3.value // 0
    flights.foreach(flight_row => acc3.add(flight_row.count))
    println("Custom accumulator = " + acc3.value )// 31390

  }
}
