package org.apache.spark.streaming.sources

import breeze.stats.distributions.Gaussian
import org.apache.spark.mllib.linalg.BLAS.axpy
import org.apache.spark.mllib.linalg.{Vector, Vectors}


case class GroundTruthDataStreamSource(dataStream: StreamSource[Vector], mu: Double = 0.0, std: Double = 1.0 ) extends StreamSource[Vector] {

  private val deviator = Gaussian(mu, std)

  private val source = {
    for {
      point <- dataStream
    } yield {
      val noiseVector = Vectors.dense(
        Array.fill(point.size)(deviator.sample())
      )

      axpy(1.0, point, noiseVector)

      noiseVector
    }
  }

  override def hasNext: Boolean = source.hasNext

  override def next(): Vector = source.next

  override def toStream: Stream[Vector] = source.toStream

}