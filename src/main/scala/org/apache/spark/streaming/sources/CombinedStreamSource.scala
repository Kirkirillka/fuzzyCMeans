package org.apache.spark.streaming.sources

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.streaming.adapters.Pipeline


case class CombinedStreamSource() extends StreamSource[Vector] {

  private val sources = List("gaussian",
    "parallel_lines",
    "circles")

  private val source = {


    val ds = sources.map(Pipeline.getDataSource).flatMap(x => x.toIterator).toIterator

    ds
  }

  override def hasNext: Boolean = source.hasNext

  override def next(): Vector = source.next

  override def toStream: Stream[Vector] = source.toStream

}