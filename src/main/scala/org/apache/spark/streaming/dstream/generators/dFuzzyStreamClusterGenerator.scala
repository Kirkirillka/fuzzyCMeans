package org.apache.spark.streaming.dstream.generators

import java.time.LocalDateTime

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.clustering.{DFuzzyStream, FuzzyCluster}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.adapters.Outputs._
import org.apache.spark.streaming.adapters.Pipeline
import org.apache.spark.streaming.dstream.generators.Utils.dFuzzyStreamDefaultClusterAlgorithmsParams._
import org.apache.spark.streaming.sources.StreamSource

case class dFuzzyStreamClusterGenerator(override val source: StreamSource[Vector],
                                        override val session: SparkSession,
                                        override val window: Int = 30,
                                        override val step: Int = 20
                               ) extends StreamGenerator[Vector](source, session, window, step ) with Logging {


  val conf: Config = ConfigFactory.load()

  private val m = conf.getDouble("streaming.algorithm.m")
  private val minK = conf.getInt("streaming.algorithm.k_min")
  private val maxK = conf.getInt("streaming.algorithm.k_max")
  private val optionalPrefix = conf.getString("streaming.option_db_prefix")
  private val useHistory = conf.getBoolean("streaming.use_history")

  /**
   * Blocking function that start streaming
   */
  override def run(): Unit = {

    val cxt = session.sparkContext

    val pointSink = _toPostgresSQL(dFuzzyStreamDataPointsSaverJDBCParams, Some(optionalPrefix))
    val clusterSink = _toPostgresSQL(dFuzzyStreamClusterSaverJDBCParams, Some(optionalPrefix))
    val predictSink = toFuzzyPredictionResultSaveToPostgreSQL(dFuzzyStreamFuzzyPredictSaverJDBCParams, Some(optionalPrefix))

    var lastClusters = Array.empty[FuzzyCluster]

    Pipeline.fromSource(source, session, window)
      // Append current timestamp
      .map(x => (LocalDateTime.now(), x))
      // Save raw data points in Postgres
      .map(x => pointSink(x._2,x._1))
      // Generate Clusters
      .map(x => {

        val timestamp = x._2
        val rdd = x._1

        // Manually change behavior
        val nextIter = useHistory match {
          case true => DFuzzyStream.train(rdd,m,lastClusters, minK,maxK)
          case false => DFuzzyStream.train(rdd,m,minK,maxK)
        }


        // use found fuzzy clusters for the next iteration
        lastClusters = nextIter.getFMiC

        // Save clusters separately to DB
        val _clusters = cxt.parallelize(lastClusters.map(_.c.vector))
        clusterSink(_clusters,timestamp)

        val fuzzyPredicts = nextIter.fuzzyPredict(rdd)

        (rdd, fuzzyPredicts,timestamp)
      })
      // Save predictions in Postgres
      .map(x => predictSink(x._1,x._2, x._3))
      .foreach(x => toFuzzyClusterPrint(x._1, x._2))
  }

}