package org.apache.spark.streaming.sources

import breeze.stats.distributions.Gaussian
import org.apache.spark.mllib.linalg.{Vector, Vectors}


case class CirclesStreamSource(circles: Int, circleRadiusMultiplication: Int = 20) extends StreamSource[Vector]{

  private val littleDeviation = Gaussian(0, 1)

  private val source = {
    for {
      line_id <- 1 to circles
      angle <- 1 to 360
    } yield Vectors.dense(
      Array(
        circleRadiusMultiplication *  line_id * Math.cos(Math.PI * angle / 180) + littleDeviation.sample(),
        circleRadiusMultiplication *  line_id * Math.sin(Math.PI * angle / 180) + littleDeviation.sample()
      )
    )
  }.toIterator

  override def hasNext: Boolean = source.hasNext

  override def next(): Vector = source.next

  override def toStream: Stream[Vector] = source.toStream

}