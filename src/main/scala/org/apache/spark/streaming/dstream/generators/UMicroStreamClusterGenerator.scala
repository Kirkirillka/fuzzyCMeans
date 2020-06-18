package org.apache.spark.streaming.dstream.generators

import com.typesafe.config.{Config, ConfigFactory}
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

  val conf: Config = ConfigFactory.load()

  private val maxK = conf.getInt("streaming.algorithm.k_max")

  /**
   * Blocking function that start streaming
   */
  override def run(): Unit = {

    val cxt = session.sparkContext

    val pointSink = _toPostgresSQL(UMicroDataPointsSaverJDBCParams)
    val clusterSink = _toPostgresSQL(UMicroClusterSaverJDBCParams)
    val predictSink = toCrispPredictionResultSaveToPostgreSQL(UMicroFuzzyPredictSaverJDBCParams)

    var lastClusters = Array.empty[UncertainCluster]

    Pipeline.fromSource(source, session, window)
      // Save raw data points in Postgres
      .map(pointSink(_))
      // Generate Clusters
      .map(rdd => {

        val nextIter = UMicro.train(rdd,maxK,lastClusters)

        // use found fuzzy clusters for the next iteration
        lastClusters = nextIter.uncertainClusters

        // Save clusters separately to DB
        val _clusters = cxt.parallelize(nextIter.uncertainClusters.map(_.expectedClusterCenter))
        clusterSink(_clusters)

        val predicts = nextIter.predict(rdd)

        (rdd, predicts)
      })
      // Save predictions in Postgres
      .map(x => predictSink(x._1,x._2))
      .foreach(x => toClusterPrint(x._1, x._2))
  }

}