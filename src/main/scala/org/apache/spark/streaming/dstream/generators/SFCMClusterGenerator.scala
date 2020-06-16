package org.apache.spark.streaming.dstream.generators

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

  val k = 4
  val m = 1
  val history = 3
  val maxIterations = 1000

  /**
   * Blocking function that start streaming
   */
  override def run(): Unit = {

    val cxt = session.sparkContext

    val pointSink = _toPostgresSQL(SFCMDataPointsSaverJDBCParams)
    val clusterSink = _toPostgresSQL(SFCMClusterSaverJDBCParams)

    var historicalModel = ArrayBuffer.empty[StreamingFuzzyCMeansModel]

    Pipeline.fromSource(source, session, window)
      // Save to Postgres
      .map(pointSink(_))
      // Generate Clusters
      .map(rdd => {

        val nextIter = StreamingFuzzyCMeans.train(rdd, k, maxIterations, historicalModel, history)

        // Additionally filter out-dated historical models
        historicalModel =  (historicalModel :+ nextIter).takeRight(history)

        // Save FuzzyClusters to DB
        val _clusters = cxt.parallelize(nextIter.clusterCenters)
        clusterSink(_clusters)

        val fuzzyPredicts = nextIter.fuzzyPredict(rdd)

        (rdd, fuzzyPredicts)
      })
      .foreach(x => toFuzzyClusterPrint(x._1, x._2))
  }

}
