package org.apache.spark.streaming.dstream.generators

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

  val m = 1
  val minK = 2
  val maxK = 5
  val history = 3
  val maxIterations = 1000

  /**
   * Blocking function that start streaming
   */
  override def run(): Unit = {

    val cxt = session.sparkContext

    val pointSink = _toPostgresSQL(dFuzzyStreamDataPointsSaverJDBCParams)
    val clusterSink = _toPostgresSQL(dFuzzyStreamClusterSaverJDBCParams)

    var lastClusters = Array.empty[FuzzyCluster]

    Pipeline.fromSource(source, session, window)
      // Save to Postgres
      .map(pointSink(_))
      // Generate Clusters
      .map(rdd => {

        val nextIter = DFuzzyStream.train(rdd, maxIterations,lastClusters,minK,maxK)

        // use found fuzzy clusters for the next iteration
        lastClusters = nextIter.getFMiC

        // Save FuzzyClusters to DB
        val _clusters = cxt.parallelize(nextIter.getFMiC.map(_.c.vector))
        clusterSink(_clusters)

        val fuzzyPredicts = nextIter.fuzzyPredict(rdd)

        (rdd, fuzzyPredicts)
      })
      .foreach(x => toFuzzyClusterPrint(x._1, x._2))
  }

}