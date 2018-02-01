package com.rama.bigdata

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

object DocsScalaLang {

    def main(args: Array[String]): Unit = {

      //
      // Futures
      // (https://www.safaribooksonline.com/library/view/Programming+Scala,+2nd+Edition/9781491950135/ch17.html#callout_tools_for_concurrency_CO1-1)
      println("Ex1")
      val futures = (0 to 9) map {
        i => Future {
          val s = i.toString
          print(s)
          s
        }
      }

      val f = Future.reduce(futures)((s1, s2) => s1 + s2)
      val n = Await.result(f, Duration.Inf)
      println("\n" + "now n:" + "\n")
      println(n)

      println("Ex2")
      val task1: Seq[Future[String]] = Seq(
        Future {
          val s = 1.toString
          println(s + "\n")
          s+1
        },
        Future {
          val s = 2.toString
          println(s + "\n")
          s
        },
        Future {
          val s = 3.toString
          println(s + "\n")
          s
        }
      )
      val aggregated1: Future[Seq[String]] = Future.sequence(task1)
      val return_values1: Seq[String] = Await.result(aggregated1, Duration.Inf)
      println("now seq of futures" +"\n")
      println(return_values1.toString())

    }
}
