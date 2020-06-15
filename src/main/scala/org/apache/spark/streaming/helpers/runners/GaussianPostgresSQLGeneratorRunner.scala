package org.apache.spark.streaming.helpers.runners

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.generators.PostgresSQLSinkStreamGenerator
import org.apache.spark.streaming.dstream.generators.Utils.{DefaultJDBCParams, JDBCParams}
import org.apache.spark.streaming.sources.GaussianStreamSource

object GaussianPostgresSQLGeneratorRunner {

  val dim = 2
  val mu = 0
  val variance = 1
  val logger = Logger(this.getClass.getName)

  def main(args: Array[String]): Unit = {

    logger.info("Loading configuration")
    val conf: Config = ConfigFactory.load()


    logger.info("""Generating Parameters for PostgresSQL.""")
    val params: JDBCParams = DefaultJDBCParams

    logger.info("Initializing Apache Spark session.")
    val ss = SparkSession.builder()
      .master(conf.getString("apache.spark.master"))
      .appName(this.getClass.getName)
      .getOrCreate()

    logger.info("Instantiating Gaussian Generator to save the data into PostgresSQL sink.")
    val source = new GaussianStreamSource(dim, mu, variance)
    val stream = new PostgresSQLSinkStreamGenerator(params,source, ss)

    // Set timeout between firing
    val timeout  = conf.getLong("generators.timeout")
    stream.timeout = timeout

    logger.info("Starting Gaussian PostgresSQL Stream!")
    stream.run()

    logger.info("Stream has been finished.")
  }
}
