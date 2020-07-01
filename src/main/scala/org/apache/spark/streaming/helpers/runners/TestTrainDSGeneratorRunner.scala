package org.apache.spark.streaming.helpers.runners

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.adapters.Pipeline


object TestTrainDSGeneratorRunner {

  private val logger = Logger(this.getClass.getName)

  def main(args: Array[String]): Unit = {

    logger.info("Loading configuration")
    val conf: Config = ConfigFactory.load()

    val clusterAlgorithmName = "train_test"

    logger.info("Initializing Apache Spark session.")
    val ss = (SparkSession builder())
      .master(conf.getString("apache.spark.master"))
      .appName(name = this.getClass.getName)
      .getOrCreate()

    logger.info(f"Use ${clusterAlgorithmName} as clustering algorithm")
    val clusterStreamGenerator = Pipeline.getClusterDSGenerator(ss, clusterAlgorithmName)

    logger.info("Starting Gaussian Kafka Stream!")
    clusterStreamGenerator.run()

    logger.info("Stream has been finished.")
  }

}
