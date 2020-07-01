package org.apache.spark.streaming.dstream.generators

import java.time.LocalDateTime

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
                                        override val session: SparkSession,
                                        override val window: Int = 30,
                                        override val step: Int =20
                               ) extends StreamGenerator[Vector](source, session, window, step) with Logging {

  val conf: Config = ConfigFactory.load()

  private val maxK = conf.getInt("streaming.algorithm.k_max")
  private val optionalPrefix = conf.getString("streaming.option_db_prefix")
  private val useHistory = conf.getBoolean("streaming.use_history")

  /**
   * Blocking function that start streaming
   */
  override def run(): Unit = {

    val cxt = session.sparkContext

    val pointSink = _toPostgresSQL(UMicroDataPointsSaverJDBCParams, Some(optionalPrefix))
    val clusterSink = _toPostgresSQL(UMicroClusterSaverJDBCParams, Some(optionalPrefix))
    val predictSink = toCrispPredictionResultSaveToPostgreSQL(UMicroFuzzyPredictSaverJDBCParams, Some(optionalPrefix))

    var lastClusters = Array.empty[UncertainCluster]

    Pipeline.fromSource(source, session, window)
      // Append current timestamp
      .map(x => (LocalDateTime.now(), x))
      // Save raw data points in Postgres
      .map(x => pointSink(x._2,x._1))
      // Generate Clusters
      .map(x => {

        val timestamp = x._2
        val rdd = x._1

        val nextIter = useHistory match {
          case true => UMicro.train(rdd,maxK,lastClusters)
          case false => UMicro.train(rdd,maxK)
        }

        // use found fuzzy clusters for the next iteration
        lastClusters = nextIter.uncertainClusters

        // Save clusters separately to DB
        val _clusters = cxt.parallelize(nextIter.uncertainClusters.map(_.expectedClusterCenter))
        clusterSink(_clusters, timestamp)

        val predicts = nextIter.predict(rdd)

        (rdd, predicts, timestamp)
      })
      // Save predictions in Postgres
      .map(x => predictSink(x._1,x._2, x._3))
      .foreach(x => toClusterPrint(x._1, x._2))
  }

}