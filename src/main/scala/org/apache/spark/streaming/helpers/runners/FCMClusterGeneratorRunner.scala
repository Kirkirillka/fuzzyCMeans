package org.apache.spark.streaming.helpers.runners

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.generators.FCMClusterGenerator
import org.apache.spark.streaming.sources.GaussianStreamSource


object FCMClusterGeneratorRunner {

  val dim = 5
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

    logger.info("Instantiating Gaussian Generator to save the data into Kafka sink.")
    val source = new GaussianStreamSource(dim, mu, variance)
    val stream = new FCMClusterGenerator(source,ss)

    // Set timeout between firing
    val timeout  = conf.getLong("generators.timeout")
    stream.timeout = timeout

    logger.info("Starting Gaussian Kafka Stream!")
    stream.run()

    logger.info("Stream has been finished.")
  }

}
