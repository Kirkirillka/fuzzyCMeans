package org.apache.spark.streaming.helpers.runners.sources

import org.apache.spark.streaming.adapters.Pipeline

object CirclesRunner {

  val source = Pipeline.getDataSource("circles")


  def main(args: Array[String]): Unit = {


    source.zipWithIndex.foreach(point => println(f"${point._2}:\t${point._1}"))

  }

}
