package org.apache.spark.streaming.helpers.runners

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.generators.FCMClusterGenerator
import org.apache.spark.streaming.sources.GaussianStreamSource


object FCMClusterGeneratorRunner {

  val dim = 3
  val mu = 2
  val variance = 3
  val logger = Logger(this.getClass.getName)

  def main(args: Array[String]): Unit = {

    logger.info("Loading configuration")
    val conf: Config = ConfigFactory.load()


    logger.info("Initializing Apache Spark session.")
    val ss = (SparkSession builder())
      .master(conf.getString("apache.spark.master"))
      .appName(name = this.getClass.getName)
      .getOrCreate()

    val source = GaussianStreamSource(dim, mu, variance)
    val stream = FCMClusterGenerator(source,ss)

    logger.info("Starting Gaussian Kafka Stream!")
    stream.run()

    logger.info("Stream has been finished.")
  }

}
