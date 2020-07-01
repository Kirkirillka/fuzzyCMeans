package org.apache.spark.streaming.sources

import breeze.stats.distributions.Gaussian
import org.apache.spark.mllib.linalg.{Vector, Vectors}


case class ParallelLinesStreamSource(lines:Int, xLimit: Int = 100) extends StreamSource[Vector] {

  private val littleDeviation = Gaussian(0, 1)

  private val source = {
    for {
      line_id <- 1 to lines
      x <- 1 to xLimit
    } yield Vectors.dense(
      Array(
        x.toDouble + littleDeviation.sample(),
        line_id * 20 + littleDeviation.sample()
      )
    )
  }.toIterator

  override def hasNext: Boolean = source.hasNext

  override def next(): Vector = source.next

  override def toStream: Stream[Vector] = source.toStream

}