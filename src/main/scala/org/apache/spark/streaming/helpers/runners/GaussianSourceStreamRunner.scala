package org.apache.spark.streaming.helpers.runners

import org.apache.spark.sql.SparkSession


object GaussianSourceStreamRunner {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("GaussianSourceStreamRunner")
      .getOrCreate()

  }

}
