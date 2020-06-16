package org.apache.spark.streaming.dstream.generators

import org.apache.spark.internal.Logging
import org.apache.spark.mllib.clustering.{UMicro, UncertainCluster}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.adapters.Outputs._
import org.apache.spark.streaming.adapters.Pipeline
import org.apache.spark.streaming.dstream.generators.Utils.UMicroDefaultClusterAlgorithmsParams._
import org.apache.spark.streaming.sources.StreamSource

case class UMicroStreamClusterGenerator(override val source: StreamSource[Vector],
                                        override val session: SparkSession, window: Int = 30
                               ) extends StreamGenerator[Vector](source, session) with Logging {

  val m = 1
  val maxK = 5
  val history = 3

  /**
   * Blocking function that start streaming
   */
  override def run(): Unit = {

    val cxt = session.sparkContext

    val pointSink = _toPostgresSQL(UMicroDataPointsSaverJDBCParams)
    val clusterSink = _toPostgresSQL(UMicroClusterSaverJDBCParams)

    var lastClusters = Array.empty[UncertainCluster]

    Pipeline.fromSource(source, session, window)
      // Save to Postgres
      .map(pointSink(_))
      // Generate Clusters
      .map(rdd => {

        val nextIter = UMicro.train(rdd,maxK,lastClusters)

        // use found fuzzy clusters for the next iteration
        lastClusters = nextIter.uncertainClusters

        // Save FuzzyClusters to DB
        val _clusters = cxt.parallelize(nextIter.uncertainClusters.map(_.expectedClusterCenter))
        clusterSink(_clusters)

        val predicts = nextIter.predict(rdd)

        (rdd, predicts)
      })
      .foreach(x => toClusterPrint(x._1, x._2))
  }

}