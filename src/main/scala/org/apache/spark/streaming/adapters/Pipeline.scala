package org.apache.spark.streaming.adapters

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.adapters.Outputs._
import org.apache.spark.streaming.dstream.generators.Utils.KafkaParams
import org.apache.spark.streaming.dstream.generators._
import org.apache.spark.streaming.sources._

case class Pipeline private(data: Iterator[RDD[Vector]]) extends Iterator[RDD[Vector]] {


  def toConsole = {
    data.foreach(_toConsole()(_))

    this
  }

  def toKafka(params: KafkaParams) = {

    val sinkFunctor = _toKafka(params)

    data.foreach(sinkFunctor(_))

    this
  }


  override def hasNext: Boolean = data.hasNext

  override def next(): RDD[Vector] = data.next()
}


object Pipeline {

  def fromSource(source: StreamSource[Vector], session: SparkSession, windows: Int = 30, step: Int = 10) = {
    Pipeline(source.toStream.sliding(windows, step).map(session.sparkContext parallelize _))
  }


  def getClusterDSGenerator(session: SparkSession, name: String = "sfcm"): StreamGenerator[Vector] = {

    val conf: Config = ConfigFactory.load()

    val dataSourceType = conf.getString("streaming.datasource")
    val window = conf.getInt("streaming.synthetic.window")
    val step = conf.getInt("streaming.synthetic.step")

    val datasource = getDataSource(dataSourceType)

    name match {
      case "fcm" => FCMClusterGenerator(datasource, session, window, step)
      case "sfcm" => SFCMClusterGenerator(datasource, session, window, step)
      case "dfuzzystream" => dFuzzyStreamClusterGenerator(datasource, session, window, step)
      case "umicro" => UMicroStreamClusterGenerator(datasource, session, window, step)
      case "train_test" => {
        val trainStreamName = conf.getString("streaming.train_test.train")
        val testStreamName = conf.getString("streaming.train_test.test")


        val trainStream = getDataSource(trainStreamName)
        val testStream = getDataSource(testStreamName)

        GroundTruth_UncertainDSGenerator(trainStream, testStream, session, window, step)
      }
    }

  }

  def getDataSource(name: String = "gaussian"): StreamSource[Vector] = {

    val conf: Config = ConfigFactory.load()

    // Gaussian stream
    val dim = conf.getInt("streaming.synthetic.dim")
    val mu = conf.getDouble("streaming.synthetic.gaussian.mu")
    val variance = conf.getDouble("streaming.synthetic.gaussian.sigma")

    // Parallel Lines stream
    val nLines = conf.getInt("streaming.synthetic.parallel_lines.n")
    val xLimit = conf.getInt("streaming.synthetic.parallel_lines.x_limit")

    // Circles stream
    val nCircles = conf.getInt("streaming.synthetic.circles.n")
    val radiusMultiplication = conf.getInt("streaming.synthetic.circles.multiplication")

    // Uncertain Data stream
    val underliedDSName = conf.getString("streaming.synthetic.uncertain_stream.ground_stream_name")
    val uncertainDSSTD = conf.getDouble("streaming.synthetic.uncertain_stream.std")
    val uncertainDSMu = conf.getDouble("streaming.synthetic.uncertain_stream.mu")


    // Ground Truth Data Stream
    val groundTruthDSName = conf.getString("streaming.synthetic.ground_truth_stream.ground_stream_name")
    val groundTruthDSSTD = conf.getDouble("streaming.synthetic.ground_truth_stream.std")
    val groundTruthDSMu = conf.getDouble("streaming.synthetic.ground_truth_stream.mu")

    val dataSource = name match {
      case "combined" => CombinedStreamSource()
      case "gaussian" => GaussianStreamSource(dim, mu, variance)
      case "parallel_lines" => ParallelLinesStreamSource(nLines, xLimit)
      case "circles" => CirclesStreamSource(nCircles, radiusMultiplication)
      case "uncertain_stream" => {
        // Safe check against recursion
        require(underliedDSName != "uncertain_stream")

        val underliedDataSource = getDataSource(underliedDSName)

        UncertainDataStreamSource(underliedDataSource, uncertainDSMu, uncertainDSSTD)

      }
      case "ground_truth_stream" => {
        // Safe check against recursion
        require(groundTruthDSName != "ground_truth_stream")

        val underliedDataSource = getDataSource(groundTruthDSName)

        GroundTruthDataStreamSource(underliedDataSource, groundTruthDSSTD, groundTruthDSMu)

      }
    }

    dataSource
  }

}