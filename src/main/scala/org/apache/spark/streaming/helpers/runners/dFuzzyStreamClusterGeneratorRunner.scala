package org.apache.spark.streaming.helpers.runners

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.generators.dFuzzyStreamClusterGenerator
import org.apache.spark.streaming.sources.GaussianStreamSource


object dFuzzyStreamClusterGeneratorRunner {

  private val logger = Logger(this.getClass.getName)

  def main(args: Array[String]): Unit = {

    logger.info("Loading configuration")
    val conf: Config = ConfigFactory.load()

    val dim = conf.getInt("streaming.synthetic.dim")
    val window = conf.getInt("streaming.synthetic.window")
    val mu = conf.getDouble("streaming.synthetic.gaussian.mu")
    val variance = conf.getDouble("streaming.synthetic.gaussian.sigma")


    logger.info("Initializing Apache Spark session.")
    val ss = (SparkSession builder())
      .master(conf.getString("apache.spark.master"))
      .appName(name = this.getClass.getName)
      .getOrCreate()

    val source = GaussianStreamSource(dim, mu, variance)
    val stream = dFuzzyStreamClusterGenerator(source,ss, window)

    logger.info("Starting Gaussian Kafka Stream!")
    stream.run()

    logger.info("Stream has been finished.")
  }

}
