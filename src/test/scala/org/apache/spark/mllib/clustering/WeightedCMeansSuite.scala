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

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.util.{LocalClusterSparkContext, MLlibTestSparkContext}
import org.apache.spark.util.Utils
import org.apache.spark.mllib.util.TestingUtils._

import scala.util.Random

/**
 * Created by acflorea on 05/04/16.
 */
class WeightedFuzzyCMeansSuite extends SparkFunSuite with MLlibTestSparkContext {

  import org.apache.spark.mllib.clustering.KMeans.{K_MEANS_PARALLEL, RANDOM}

  test("two clusters") {
    val points = Seq(
      Vectors.dense(0.0, 0.0),
      Vectors.dense(0.0, 0.1),
      Vectors.dense(0.1, 0.0),
      Vectors.dense(9.0, 0.0),
      Vectors.dense(9.0, 0.2),
      Vectors.dense(9.2, 0.0)
    )
    val rdd = sc.parallelize(points, 3).cache()

    for (initMode <- Seq(RANDOM, K_MEANS_PARALLEL)) {

      (1 to 2).map(_ * 2) foreach { fuzzifier =>

        val model = WeightedFuzzyCMeans.train(rdd, k = 2, maxIterations = 10, runs = 10, initMode,
          seed = 26031979L, m = fuzzifier)

        assert(model.m === fuzzifier)

        val fuzzyPredicts = model.fuzzyPredict(rdd).collect()

        assert(fuzzyPredicts(0).maxBy(_._2)._1 === fuzzyPredicts(1).maxBy(_._2)._1)
        assert(fuzzyPredicts(0).maxBy(_._2)._1 === fuzzyPredicts(2).maxBy(_._2)._1)
        assert(fuzzyPredicts(3).maxBy(_._2)._1 === fuzzyPredicts(4).maxBy(_._2)._1)
        assert(fuzzyPredicts(3).maxBy(_._2)._1 === fuzzyPredicts(5).maxBy(_._2)._1)
        assert(fuzzyPredicts(0).maxBy(_._2)._1 != fuzzyPredicts(3).maxBy(_._2)._1)

      }
    }
  }



  test("use pre trained model as initial") {
    val points = Seq(
      Vectors.dense(0.0, 0.0),
      Vectors.dense(0.0, 0.1),
      Vectors.dense(0.1, 0.0),
      Vectors.dense(9.0, 0.0),
      Vectors.dense(9.0, 0.2),
      Vectors.dense(9.2, 0.0)
    )
    val rdd = sc.parallelize(points, 3).cache()

    val pre_model = WeightedFuzzyCMeans.train(rdd, k = 2, maxIterations = 10, runs = 10)

    val model = WeightedFuzzyCMeans.train(rdd, k = 2, maxIterations = 10, runs = 10, pre_model)


    val fuzzyPredicts_pre = model.fuzzyPredict(rdd).collect()
    val fuzzyPredicts = model.fuzzyPredict(rdd).collect()

    assert(fuzzyPredicts_pre.length === fuzzyPredicts.length)
  }


  test("more clusters than points") {
    val data = sc.parallelize(
      Array(
        Vectors.dense(1.0, 2.0, 3.0),
        Vectors.dense(1.0, 3.0, 4.0)),
      2)

    // Make sure code runs.
    var model = WeightedFuzzyCMeans.train(data, k = 3, maxIterations = 1)
    assert(model.clusterCenters.length === 3)

  }

  test("single cluster with big dataset") {
    val smallData = Array(
      Vectors.dense(1.0, 2.0, 6.0),
      Vectors.dense(1.0, 3.0, 0.0),
      Vectors.dense(1.0, 4.0, 6.0)
    )
    val data = sc.parallelize((1 to 100).flatMap(_ => smallData), 4)

    // No matter how many runs or iterations we use, we should get one cluster,
    // centered at the mean of the points

    val center = Vectors.dense(1.0, 3.0, 4.0)

    var model = WeightedFuzzyCMeans.train(data, k = 1, maxIterations = 1)
    assert(model.clusterCenters.length === 1)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = WeightedFuzzyCMeans.train(data, k = 1, maxIterations = 2)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = WeightedFuzzyCMeans.train(data, k = 1, maxIterations = 5)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = WeightedFuzzyCMeans.train(data, k = 1, maxIterations = 1, runs = 5)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = WeightedFuzzyCMeans.train(data, k = 1, maxIterations = 1, runs = 5)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = WeightedFuzzyCMeans.train(data, k = 1, maxIterations = 1, runs = 1, initializationMode = RANDOM)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = WeightedFuzzyCMeans.train(data, k = 1, maxIterations = 1, runs = 1,
      initializationMode = K_MEANS_PARALLEL)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

  }

  test("single cluster with sparse data") {

    val n = 10000
    val data = sc.parallelize((1 to 100).flatMap { i =>
      val x = i / 1000.0
      Array(
        Vectors.sparse(n, Seq((0, 1.0 + x), (1, 2.0), (2, 6.0))),
        Vectors.sparse(n, Seq((0, 1.0 - x), (1, 2.0), (2, 6.0))),
        Vectors.sparse(n, Seq((0, 1.0), (1, 3.0 + x))),
        Vectors.sparse(n, Seq((0, 1.0), (1, 3.0 - x))),
        Vectors.sparse(n, Seq((0, 1.0), (1, 4.0), (2, 6.0 + x))),
        Vectors.sparse(n, Seq((0, 1.0), (1, 4.0), (2, 6.0 - x)))
      )
    }, 4)

    data.persist()

    // No matter how many runs or iterations we use, we should get one cluster,
    // centered at the mean of the points

    val center = Vectors.sparse(n, Seq((0, 1.0), (1, 3.0), (2, 4.0)))

    var model = WeightedFuzzyCMeans.train(data, k = 1, maxIterations = 1)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = WeightedFuzzyCMeans.train(data, k = 1, maxIterations = 2)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = WeightedFuzzyCMeans.train(data, k = 1, maxIterations = 5)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = WeightedFuzzyCMeans.train(data, k = 1, maxIterations = 1, runs = 5)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = WeightedFuzzyCMeans.train(data, k = 1, maxIterations = 1, runs = 5)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = WeightedFuzzyCMeans.train(data, k = 1, maxIterations = 1, runs = 1, initializationMode = RANDOM)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    model = WeightedFuzzyCMeans.train(data, k = 1, maxIterations = 1, runs = 1,
      initializationMode = K_MEANS_PARALLEL)
    assert(model.clusterCenters.head ~== center absTol 1E-5)

    data.unpersist()
  }

//  test("model save/load") {
//    val tempDir = Utils.createTempDir()
//    val path = tempDir.toURI.toString
//
//    Array(true, false).foreach { case selector =>
//      val model = FuzzyCSuite.createModel(10, 3, selector)
//      // Save model, load it back, and compare.
//      try {
//        model.save(sc, path)
//        val sameModel = WeightedFuzzyCMeansModel.load(sc, path)
//        FuzzyCSuite.checkEqual(model, sameModel)
//      } finally {
//        Utils.deleteRecursively(tempDir)
//      }
//    }
//  }

}


object WeightedFuzzyCSuite extends SparkFunSuite {
  def createModel(dim: Int, k: Int, isSparse: Boolean): WeightedFuzzyCMeansModel = {
    val singlePoint = isSparse match {
      case true =>
        Vectors.sparse(dim, Array.empty[Int], Array.empty[Double])
      case _ =>
        Vectors.dense(Array.fill[Double](dim)(0.0))
    }
    new WeightedFuzzyCMeansModel(Array.fill[Vector](k)(singlePoint),Vectors.dense(Array.fill(k)(1.0)), 2)
  }

  def checkEqual(a: WeightedFuzzyCMeansModel, b: WeightedFuzzyCMeansModel): Unit = {
    assert(a.k === b.k)
    assert(a.m === b.m)
    a.clusterCenters.zip(b.clusterCenters).foreach {
      case (ca: SparseVector, cb: SparseVector) =>
        assert(ca === cb)
      case (ca: DenseVector, cb: DenseVector) =>
        assert(ca === cb)
      case _ =>
        throw new AssertionError("checkEqual failed since the two clusters were not identical.\n")
    }
  }
}

class WeightedFuzzyCMeansClusterSuite extends SparkFunSuite with LocalClusterSparkContext {

  test("task size should be small in both training and prediction") {
    val m = 4
    val n = 200000
    val points = sc.parallelize(0 until m, 2).mapPartitionsWithIndex { (idx, iter) =>
      val random = new Random(idx)
      iter.map(i => Vectors.dense(Array.fill(n)(random.nextDouble)))
    }.cache()
    for (initMode <- Seq(KMeans.RANDOM, KMeans.K_MEANS_PARALLEL)) {
      // If we serialize data directly in the task closure, the size of the serialized task would be
      // greater than 1MB and hence Spark would throw an error.
      val model = WeightedFuzzyCMeans.train(points, 2, 2, 1, initMode)
      val predictions = model.fuzzyPredict(points).collect()
      val hard_predictions = model.predict(points).collect()
      val cost = model.computeCost(points)
    }
  }
}

/**
 * Leave this here to be able to compare the "standard" KMeans with the enhanced version
 * in terms of performance
 */
class WeightedKMeansClusterSuite extends SparkFunSuite with LocalClusterSparkContext {

  test("task size should be small in both training and prediction") {
    val m = 4
    val n = 200000
    val points = sc.parallelize(0 until m, 2).mapPartitionsWithIndex { (idx, iter) =>
      val random = new Random(idx)
      iter.map(i => Vectors.dense(Array.fill(n)(random.nextDouble)))
    }.cache()
    for (initMode <- Seq(KMeans.RANDOM, KMeans.K_MEANS_PARALLEL)) {
      // If we serialize data directly in the task closure, the size of the serialized task would be
      // greater than 1MB and hence Spark would throw an error.
      val model = KMeans.train(points, 2, 2, 1, initMode)
      val predictions = model.predict(points).collect()
      val cost = model.computeCost(points)
    }
  }
}
