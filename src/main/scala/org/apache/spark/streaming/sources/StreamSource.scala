package org.apache.spark.streaming.sources

abstract class StreamSource[+T](private val dim: Int) extends Iterator[T] {

  def toStream: Stream[T]
}
