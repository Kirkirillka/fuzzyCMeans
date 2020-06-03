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

/**
 * A clustering model for Fuzzy C-means. Each point to each cluster with a certain degree of probability
 */
class UMicroModel(val uncertainClusters: Array[UncertainCluster])
  extends Serializable with PMMLExportable {

  /**
   * Returns the cluster index that a given point belongs to.
   */
  def predict(point: Vector): Int = {
    UMicro.findClosest(clusterCenters, new VectorWithNorm(point))._1
  }

  /**
   * Maps given points to their cluster indices.
   */
  def predict(points: RDD[Vector]): RDD[Int] = {
    val centersWithNorm = clusterCenters
    val bcCenters = points.context.broadcast(centersWithNorm)
    points.map(p => UMicro.findClosest(bcCenters.value, new VectorWithNorm(p))._1)
  }

  /**
   * Maps given points to their cluster indices.
   */
  def predict(points: JavaRDD[Vector]): JavaRDD[java.lang.Integer] =
    predict(points.rdd).toJavaRDD().asInstanceOf[JavaRDD[java.lang.Integer]]

  private def clusterCenters: Iterable[VectorWithNorm] =
    uncertainClusters.map(_.expectedClusterCenter).map(new VectorWithNorm(_))

}
