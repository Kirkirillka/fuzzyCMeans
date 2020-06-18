package org.apache.spark.streaming.dstream.generators

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.clustering.FuzzyCMeans
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.adapters.Outputs._
import org.apache.spark.streaming.adapters.Pipeline
import org.apache.spark.streaming.dstream.generators.Utils._
import org.apache.spark.streaming.sources.StreamSource

case class FCMClusterGenerator(override val source: StreamSource[Vector],
                          override val session: SparkSession, window: Int = 30
                         ) extends StreamGenerator[Vector](source, session) with Logging {

  val conf: Config = ConfigFactory.load()

  private val k = conf.getInt("streaming.algorithm.k_required")
  private val m = conf.getDouble("streaming.algorithm.m")
  private val maxIterations = conf.getInt("streaming.algorithm.max_iterations")

  /**
   * Blocking function that start streaming
   */
  override def run(): Unit = {

    val cxt = session.sparkContext

    val pointSink = _toPostgresSQL(FCMDataPointsSaverJDBCParams)
    val clusterSink = _toPostgresSQL(FCMClusterSaverJDBCParams)
    val predictSink = toFuzzyPredictionResultSaveToPostgreSQL(FCMFuzzyPredictSaverJDBCParams)

    Pipeline.fromSource(source, session, window)
      // Save raw data points in Postgres
      .map(pointSink(_))
      // Generate Clusters
      .map(rdd => {

      val nextIter = FuzzyCMeans.train(rdd,k,m,maxIterations)

      // Save clusters separately to DB
      val _clusters = cxt.parallelize(nextIter.clusterCenters)
      clusterSink(_clusters)

      val fuzzyPredicts = nextIter.fuzzyPredict(rdd)

       (rdd,fuzzyPredicts)
      })
      // Save predictions in Postgres
      .map(x => predictSink(x._1,x._2))
      .foreach(x => toFuzzyClusterPrint(x._1, x._2))
  }

}
