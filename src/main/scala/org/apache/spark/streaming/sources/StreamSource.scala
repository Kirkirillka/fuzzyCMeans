package org.apache.spark.streaming.sources

abstract class StreamSource[+T] extends Iterator[T] {

  def toStream: Stream[T]
}
