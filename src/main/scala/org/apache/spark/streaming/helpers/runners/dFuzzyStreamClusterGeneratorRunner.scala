package org.apache.spark.streaming.helpers.runners

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.adapters.Pipeline
import org.apache.spark.streaming.dstream.generators.dFuzzyStreamClusterGenerator


object dFuzzyStreamClusterGeneratorRunner {

  private val logger = Logger(this.getClass.getName)

  def main(args: Array[String]): Unit = {

    logger.info("Loading configuration")
    val conf: Config = ConfigFactory.load()

    val window = conf.getInt("streaming.synthetic.window")
    val step = conf.getInt("streaming.synthetic.step")
    val data_type = conf.getString("streaming.datasource")


    logger.info("Initializing Apache Spark session.")
    val ss = (SparkSession builder())
      .master(conf.getString("apache.spark.master"))
      .appName(name = this.getClass.getName)
      .getOrCreate()

    val source = Pipeline.getDataSource(data_type)
    val stream = dFuzzyStreamClusterGenerator(source,ss, window,step)

    logger.info("Starting Gaussian Kafka Stream!")
    stream.run()

    logger.info("Stream has been finished.")
  }

}
