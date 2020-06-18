package org.apache.spark.streaming.dstream.generators

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
                                        override val session: SparkSession, window: Int = 30
                               ) extends StreamGenerator[Vector](source, session) with Logging {


  val conf: Config = ConfigFactory.load()

  private val m = conf.getDouble("streaming.algorithm.m")
  private val minK = conf.getInt("streaming.algorithm.k_min")
  private val maxK = conf.getInt("streaming.algorithm.k_max")

  /**
   * Blocking function that start streaming
   */
  override def run(): Unit = {

    val cxt = session.sparkContext

    val pointSink = _toPostgresSQL(dFuzzyStreamDataPointsSaverJDBCParams)
    val clusterSink = _toPostgresSQL(dFuzzyStreamClusterSaverJDBCParams)
    val predictSink = toFuzzyPredictionResultSaveToPostgreSQL(dFuzzyStreamFuzzyPredictSaverJDBCParams)

    var lastClusters = Array.empty[FuzzyCluster]

    Pipeline.fromSource(source, session, window)
      // Save raw data points in Postgres
      .map(pointSink(_))
      // Generate Clusters
      .map(rdd => {

        val nextIter = DFuzzyStream.train(rdd,m,lastClusters,minK,maxK)

        // use found fuzzy clusters for the next iteration
        lastClusters = nextIter.getFMiC

        // Save clusters separately to DB
        val _clusters = cxt.parallelize(nextIter.getFMiC.map(_.c.vector))
        clusterSink(_clusters)

        val fuzzyPredicts = nextIter.fuzzyPredict(rdd)

        (rdd, fuzzyPredicts)
      })
      // Save predictions in Postgres
      .map(x => predictSink(x._1,x._2))
      .foreach(x => toFuzzyClusterPrint(x._1, x._2))
  }

}