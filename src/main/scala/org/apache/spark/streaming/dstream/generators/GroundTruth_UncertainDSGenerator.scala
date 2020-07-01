package org.apache.spark.streaming.dstream.generators

import java.time.LocalDateTime

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.clustering.FuzzyCMeans
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.adapters.Outputs._
import org.apache.spark.streaming.adapters.Pipeline
import org.apache.spark.streaming.dstream.generators.Utils.UncertainStreamDefaultClusterAlgorithmsParams.{_}
import org.apache.spark.streaming.dstream.generators.Utils.GroundTruthStreamDefaultClusterAlgorithmsParams.{_}
import org.apache.spark.streaming.sources.StreamSource

case class GroundTruth_UncertainDSGenerator private(val train: StreamSource[Vector],
                                                    val test: StreamSource[Vector],
                                                    override val session: SparkSession,
                                                    override val window: Int = 30,
                                                    override val step: Int = 30
                              ) extends StreamGenerator[Vector](test, session, window, step) with Logging {

  val conf: Config = ConfigFactory.load()

  private val k = conf.getInt("streaming.algorithm.k_required")
  private val m = conf.getDouble("streaming.algorithm.m")
  private val maxIterations = conf.getInt("streaming.algorithm.max_iterations")

  /**
   * Blocking function that start streaming
   */
  override def run(): Unit = {

    val cxt = session.sparkContext

    // Uncertain Stream
    val train_pointSink = _toPostgresSQL(UncertainStreamDataPointsSaverJDBCParams)
    val train_clusterSink = _toPostgresSQL(UncertainStreamClusterSaverJDBCParams)
    val train_predictSink = toFuzzyPredictionResultSaveToPostgreSQL(UncertainStreamFuzzyPredictSaverJDBCParams)

    // Ground Trunth
    val test_pointSink = _toPostgresSQL(GroundTruthStreamDataPointsSaverJDBCParams)
    val test_clusterSink = _toPostgresSQL(GroundTruthStreamClusterSaverJDBCParams)
    val test_predictSink = toFuzzyPredictionResultSaveToPostgreSQL(GroundTruthStreamFuzzyPredictSaverJDBCParams)

    // Left - uncertain DS
    // Right - ground truth
    (Pipeline.fromSource(train, session, window, step)  zip Pipeline.fromSource(test, session, window, step))

      // Append current timestamp
      .map(x => (LocalDateTime.now(), x))
      // Save raw data points in Postgres
      .map(x => (train_pointSink(x._2._1,x._1),test_pointSink(x._2._2, x._1)))
      // Generate Clusters
      .map(x => {

        val train_timestamp = x._1._2
        val test_timestamp = x._2._2
        val train_rdd = x._1._1
        val test_rdd = x._2._1

        // Train section
        ////////
        val nextIter = FuzzyCMeans.train(train_rdd, k, m, maxIterations)
        // Get prediction for train section
        val train_fuzzyPredicts = nextIter.fuzzyPredict(train_rdd)
        /////////

        // Test section
        /////////
        // Get prediction for test section
        val test_fuzzyPredicts = nextIter.fuzzyPredict(test_rdd)
        /////////


        //Left - Uncertain/Test
        //Right - GroundTruth/Train
        ((train_rdd, train_fuzzyPredicts,train_timestamp), (test_rdd, test_fuzzyPredicts,test_timestamp))
      })
      // Save predictions in Postgres
      .foreach(x => {
        val (left, right) = (x._1, x._2)

        train_predictSink(left._1,left._2, left._3)
        test_predictSink(right._1,right._2, right._3)

      })
  }

}


