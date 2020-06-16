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
                                val sigma: Double) extends StreamSource[Vector](dim){

  /**
   * a real Gaussian  (mu, sigma) to use
   */
  val dist = new Gaussian(mu, sigma)

  // Anytime returns true to indicate there always something in line
  override def hasNext: Boolean = true

  /**
   * A 'dim' - dimensional sample from Gaussian distribution (mu, sigma)
   * @return a 'dim' dimensional Vector
   */
  override def next(): Vector = Vectors.dense(dist.sample(dim).toArray)


  override def toStream = Stream.continually(this.next())

}