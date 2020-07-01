package org.apache.spark.streaming.dstream.generators

import java.time.LocalDateTime

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
                                override val session: SparkSession,
                                override val window: Int = 30,
                                override val step: Int = 20
                               ) extends StreamGenerator[Vector](source, session, window, step) with Logging {

  val conf: Config = ConfigFactory.load()

  private val k = conf.getInt("streaming.algorithm.k_required")
  private val m = conf.getDouble("streaming.algorithm.m")
  private val history = conf.getInt("streaming.algorithm.history")
  private val maxIterations = conf.getInt("streaming.algorithm.max_iterations")
  private val optionalPrefix = conf.getString("streaming.option_db_prefix")
  private val useHistory = conf.getBoolean("streaming.use_history")

  /**
   * Blocking function that start streaming
   */
  override def run(): Unit = {

    val cxt = session.sparkContext

    val pointSink = _toPostgresSQL(SFCMDataPointsSaverJDBCParams, Some(optionalPrefix))
    val clusterSink = _toPostgresSQL(SFCMClusterSaverJDBCParams, Some(optionalPrefix))
    val predictSink = toFuzzyPredictionResultSaveToPostgreSQL(SFCMFuzzyPredictSaverJDBCParams, Some(optionalPrefix))

    var historicalModel = ArrayBuffer.empty[StreamingFuzzyCMeansModel]

    Pipeline.fromSource(source, session, window, step)
      // Append current timestamp
      .map(x => (LocalDateTime.now(), x))
      // Save raw data points in Postgres
      .map(x => pointSink(x._2,x._1))
      // Generate Clusters
      .map(x => {

        val timestamp = x._2
        val rdd = x._1

        val nextIter = useHistory match {
          case true =>  StreamingFuzzyCMeans.train(rdd, k, maxIterations, historicalModel, history, m)
          case false =>   StreamingFuzzyCMeans.train(rdd, k, maxIterations,m)
        }


        // Additionally filter out-dated historical models
        historicalModel = (historicalModel :+ nextIter).takeRight(history)

        // Save clusters separately to DB
        val _clusters = cxt.parallelize(nextIter.clusterCenters)
        clusterSink(_clusters,timestamp)

        val fuzzyPredicts = nextIter.fuzzyPredict(rdd)

        (rdd, fuzzyPredicts,timestamp)
      })
      // Save predictions in Postgres
      .map(x => predictSink(x._1, x._2, x._3))
      .foreach(x => toFuzzyClusterPrint(x._1, x._2))
  }

}
