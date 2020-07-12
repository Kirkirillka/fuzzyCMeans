package org.apache.spark.streaming.helpers.runners

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.adapters.Pipeline
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.InputStreamReader

import org.apache.spark.streaming.Utils.getConfig


object ClusterGeneratorRunner {

  private val logger = Logger(this.getClass.getName)

  def main(args: Array[String]): Unit = {

    logger.info("Loading configuration")

    val conf: Config = args match {
      case a if a.length> 0 => getConfig(Some(a(0)))
      case _ => getConfig()
    }

    val clusterAlgorithmName = conf.getString("streaming.cluster_algorithm")

    logger.info("Initializing Apache Spark session.")
    val ss = (SparkSession builder())
      .master(conf.getString("apache.spark.master"))
      .appName(name = this.getClass.getName)
      .getOrCreate()

    logger.info(f"Use $clusterAlgorithmName as clustering algorithm")
    val clusterStreamGenerator = Pipeline.getClusterDSGenerator(ss, clusterAlgorithmName)

    logger.info("Starting Gaussian Kafka Stream!")
    clusterStreamGenerator.run()

    logger.info("Stream has been finished.")
  }

}
