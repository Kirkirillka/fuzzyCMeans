package org.apache.spark.streaming.sources

import breeze.stats.distributions.Gaussian
import org.apache.spark.mllib.linalg.{Vectors, Vector}


/**
 * A class to continuously pool from GaussianStream
 * @param dim - dimension of each entry returned
 * @param mu - mean
 * @param sigma - variance
 */
case class GaussianStreamSource(val dim: Int,
                                val mu: Double,
                                val sigma: Double,
                                val limit: Int = 1000) extends StreamSource[Vector]{

  /**
   * a real Gaussian  (mu, sigma) to use
   */
  private val dist = new Gaussian(mu, sigma)

  private def get_sample =  Vectors.dense(dist.sample(dim).toArray)

  private val source = Iterator.continually(get_sample).take(limit)

  // Anytime returns true to indicate there always something in line
  override def hasNext: Boolean = source.hasNext

  /**
   * A 'dim' - dimensional sample from Gaussian distribution (mu, sigma)
   * @return a 'dim' dimensional Vector
   */
  override def next(): Vector = source.next


  override def toStream: Stream[Vector] = source.toStream

}