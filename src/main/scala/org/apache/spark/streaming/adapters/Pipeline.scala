package org.apache.spark.streaming.adapters

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.adapters.Outputs._
import org.apache.spark.streaming.dstream.generators.Utils.{JDBCParams, KafkaParams}
import org.apache.spark.streaming.sources.StreamSource

case class Pipeline private (data: Iterator[RDD[Vector]]) extends Iterator[RDD[Vector]] {


  def toConsole = {
    data.foreach(_toConsole()(_))

    this
  }

  def toKafka(params: KafkaParams) = {

    val sinkFunctor = _toKafka(params)

    data.foreach(sinkFunctor(_))

    this
  }

  def toPostgresSQL(params: JDBCParams) = {

    val sinkFunctor = _toPostgresSQL(params)
    data.map(sinkFunctor(_))

    this
  }

  override def hasNext: Boolean = data.hasNext

  override def next(): RDD[Vector] = data.next()
}


object Pipeline {

  def fromSource(source: StreamSource[Vector], session: SparkSession, windows: Int = 30) = {
    Pipeline(source.toStream.sliding(windows).map(session.sparkContext parallelize _))
  }

}