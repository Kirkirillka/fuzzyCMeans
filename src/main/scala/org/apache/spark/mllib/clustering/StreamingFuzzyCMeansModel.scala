/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.mllib.clustering

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.pmml.PMMLExportable
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * A clustering model for Streaming Fuzzy C-means. Each point to each cluster with a certain degree of probability
 */
class StreamingFuzzyCMeansModel(val clusterCenters: Array[Vector],
                                val weights: Vector,
                                val historicalModels: ArrayBuffer[StreamingFuzzyCMeansModel],
                                val m: Double = 2.0)
  extends Serializable with PMMLExportable {

  /**
   * Total number of clusters.
   */
  def k: Int = clusterCenters.length

  /**
   * Returns the cluster index that a given point belongs to.
   */
  def predict(point: Vector): Int = {
    StreamingFuzzyCMeans.findClosest(clusterCentersWithNorm, new VectorWithNorm(point))._1
  }

  /**
   * For each cluster, returns its index
   * and the probability for the point to belong to that particular cluster
   */
  def fuzzyPredict(point: Vector): Seq[(Int, Double)] = {
    val centersWithNorm = clusterCentersWithNorm.toArray
    val degreesOfMembership = StreamingFuzzyCMeans.degreesOfMembership(
      centersWithNorm,
      new VectorWithNorm(point),
      m)._1
    degreesOfMembership.zipWithIndex.map(_.swap)
  }

  /**
   * Maps given points to their cluster indices.
   */
  def predict(points: RDD[Vector]): RDD[Int] = {
    val centersWithNorm = clusterCentersWithNorm
    val bcCentersWithNorm = points.context.broadcast(centersWithNorm)
    points.map(p => StreamingFuzzyCMeans.findClosest(bcCentersWithNorm.value, new VectorWithNorm(p))._1)
  }

  /**
   * Maps given points to their cluster indices.
   */
  def fuzzyPredict(points: RDD[Vector]): RDD[Seq[(Int, Double)]] = {
    val centersWithNorm = clusterCentersWithNorm
    val bcCentersWithNorm = points.context.broadcast(centersWithNorm)
    val bcm = points.context.broadcast(m)
    points.map { p =>
      val localCentersWithNorm = bcCentersWithNorm.value.toArray
        // Sort clusters by distance from the beginning of coordinates
        // Helps to persist order for iterating runs
        .sortBy(_.norm)
      val localM = bcm.value
      StreamingFuzzyCMeans.degreesOfMembership(
        localCentersWithNorm,
        new VectorWithNorm(p),
        localM)._1.zipWithIndex.map(_.swap)
    }
  }

  /**
   * Maps given points to their cluster indices.
   */
  def predict(points: JavaRDD[Vector]): JavaRDD[java.lang.Integer] =
    predict(points.rdd).toJavaRDD().asInstanceOf[JavaRDD[java.lang.Integer]]

  /**
   * Return the cost (sum of squared distances of points to their nearest center) for this
   * model on the given data.
   */
  def computeCost(data: RDD[Vector]): Double = {
    val centersWithNorm = clusterCentersWithNorm
    val bcCentersWithNorm = data.context.broadcast(centersWithNorm)
    data.map(p => StreamingFuzzyCMeans.pointCost(bcCentersWithNorm.value, new VectorWithNorm(p))).sum()
  }

  private def clusterCentersWithNorm: Iterable[VectorWithNorm] =
    clusterCenters.map(new VectorWithNorm(_))

}
