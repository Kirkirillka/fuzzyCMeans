package org.apache.spark.streaming.dstream.generators

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.clustering.{StreamingFuzzyCMeans, StreamingFuzzyCMeansModel}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.adapters.Outputs._
import org.apache.spark.streaming.adapters.Pipeline
import org.apache.spark.streaming.dstream.generators.Utils.SFCMDefaultClusterAlgorithmsParams._
import org.apache.spark.streaming.sources.StreamSource

import scala.collection.mutable.ArrayBuffer


case class SFCMClusterGenerator(override val source: StreamSource[Vector],
                                override val session: SparkSession, window: Int = 30
                               ) extends StreamGenerator[Vector](source, session) with Logging {

  val conf: Config = ConfigFactory.load()

  private val k = conf.getInt("streaming.algorithm.k_required")
  private val m = conf.getDouble("streaming.algorithm.m")
  private val history = conf.getInt("streaming.algorithm.history")
  private val maxIterations = conf.getInt("streaming.algorithm.max_iterations")

  /**
   * Blocking function that start streaming
   */
  override def run(): Unit = {

    val cxt = session.sparkContext

    val pointSink = _toPostgresSQL(SFCMDataPointsSaverJDBCParams)
    val clusterSink = _toPostgresSQL(SFCMClusterSaverJDBCParams)
    val predictSink = toFuzzyPredictionResultSaveToPostgreSQL(SFCMFuzzyPredictSaverJDBCParams)

    var historicalModel = ArrayBuffer.empty[StreamingFuzzyCMeansModel]

    Pipeline.fromSource(source, session, window)
      // Save raw data points in Postgres
      .map(pointSink(_))
      // Generate Clusters
      .map(rdd => {

        val nextIter = StreamingFuzzyCMeans.train(rdd, k ,maxIterations, historicalModel, history,m)

        // Additionally filter out-dated historical models
        historicalModel =  (historicalModel :+ nextIter).takeRight(history)

        // Save clusters separately to DB
        val _clusters = cxt.parallelize(nextIter.clusterCenters)
        clusterSink(_clusters)

        val fuzzyPredicts = nextIter.fuzzyPredict(rdd)

        (rdd, fuzzyPredicts)
      })
      // Save predictions in Postgres
      .map(x => predictSink(x._1,x._2))
      .foreach(x => toFuzzyClusterPrint(x._1, x._2))
  }

}
