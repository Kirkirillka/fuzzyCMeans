package org.apache.spark.streaming.dstream.generators

import java.time.LocalDateTime

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.clustering.FuzzyCMeans
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.adapters.Outputs._
import org.apache.spark.streaming.adapters.Pipeline
import org.apache.spark.streaming.dstream.generators.Utils._
import org.apache.spark.streaming.sources.StreamSource

case class FCMClusterGenerator private (override val source: StreamSource[Vector],
                               override val session: SparkSession,
                               override val window: Int = 30,
                               override val step: Int = 30
                              ) extends StreamGenerator[Vector](source, session, window, step) with Logging {

  val conf: Config = ConfigFactory.load()

  private val k = conf.getInt("streaming.algorithm.k_required")
  private val m = conf.getDouble("streaming.algorithm.m")
  private val maxIterations = conf.getInt("streaming.algorithm.max_iterations")
  private val optionalPrefix = conf.getString("streaming.option_db_prefix")

  /**
   * Blocking function that start streaming
   */
  override def run(): Unit = {

    val cxt = session.sparkContext

    val pointSink = _toPostgresSQL(FCMDataPointsSaverJDBCParams,Some(optionalPrefix))
    val clusterSink = _toPostgresSQL(FCMClusterSaverJDBCParams, Some(optionalPrefix))
    val predictSink = toFuzzyPredictionResultSaveToPostgreSQL(FCMFuzzyPredictSaverJDBCParams, Some(optionalPrefix))

    Pipeline.fromSource(source, session, window, step)
      // Append current timestamp
      .map(x => (LocalDateTime.now(), x))
      // Save raw data points in Postgres
      .map(x => pointSink(x._2,x._1))
      // Generate Clusters
      .map(x => {

        val timestamp = x._2
        val rdd = x._1

        val nextIter = FuzzyCMeans.train(rdd, k, m, maxIterations)

        // Save clusters separately to DB
        val _clusters = cxt.parallelize(nextIter.clusterCenters)
        clusterSink(_clusters,timestamp)

        val fuzzyPredicts = nextIter.fuzzyPredict(rdd)

        (rdd, fuzzyPredicts,timestamp)
      })
      // Save predictions in Postgres
      .map(x => predictSink(x._1,x._2, x._3))
      .foreach(x => toFuzzyClusterPrint(x._1, x._2))
  }

}


