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

import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg.BLAS.{axpy, scal}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.random.XORShiftRandom
import org.apache.spark.util.{AccumulatorV2, Utils}

import scala.collection.mutable.ArrayBuffer


/**
 *
 * Weighted Fuzzy C-Means is a modification of FCM.
 *
 * @param k                   number of clusters
 * @param maxIterations       max number of iterations
 * @param runs                number of parallel runs, defaults to 1. The best model is returned.
 * @param initializationMode  initialization model, either "random" or "k-means||" (default).
 * @param initializationSteps Number of steps for the k-means|| initialization mode
 * @param epsilon             Threshold in membership values to consider convergence
 * @param seed                random seed value for cluster initialization
 */
class WeightedFuzzyCMeans private(
                                   private var k: Int,
                                   private var m: Double,
                                   private var maxIterations: Int,
                                   private var runs: Int,
                                   private var initializationMode: String,
                                   private var initializationSteps: Int,
                                   private var epsilon: Double,
                                   private var seed: Long) extends Serializable with Logging {


  /**
   * Constructs a FuzzyCMeans instance with default parameters: {k: 2, m: 2, maxIterations: 20, runs: 1,
   * initializationMode: "k-means||", initializationSteps: 5, epsilon: 1e-4, seed: random}.
   */
  def this() = this(2, 2, 20, 1, KMeans.K_MEANS_PARALLEL, 5, 1e-4, Utils.random.nextLong())

  /**
   * Number of clusters to create (k).
   */
  def getK: Int = k

  /**
   * Set the number of clusters to create (k). Default: 2.
   */
  def setK(k: Int): this.type = {
    this.k = k
    this
  }

  /**
   * The level of cluster fuzziness (m)
   * Check -> https://en.wikipedia.org/wiki/Fuzzy_clustering
   */
  def getM: Double = m

  /**
   * Set the level of cluster fuzziness (m). Default: 2 - most used value
   * For m = 1, the used algorithm is the hard KMeans
   */
  def setM(m: Double): this.type = {
    if (m < 1) {
      throw new IllegalArgumentException("Fuzzifier must be greater or equal with 1!")
    }
    this.m = m
    this
  }

  /**
   * Maximum number of iterations to run.
   */
  def getMaxIterations: Int = maxIterations

  /**
   * Set maximum number of iterations to run. Default: 20.
   */
  def setMaxIterations(maxIterations: Int): this.type = {
    this.maxIterations = maxIterations
    this
  }

  /**
   * The initialization algorithm. This can be either "random" or "k-means||".
   */
  def getInitializationMode: String = initializationMode

  /**
   * Set the initialization algorithm. This can be either "random" to choose random points as
   * initial cluster centers, or "k-means||" to use a parallel variant of k-means++
   * (Bahmani et al., Scalable K-Means++, VLDB 2012). Default: k-means||.
   */
  def setInitializationMode(initializationMode: String): this.type = {
    KMeans.validateInitMode(initializationMode)
    this.initializationMode = initializationMode
    this
  }

  /**
   * :: Experimental ::
   * Number of runs of the algorithm to execute in parallel.
   */
  // @deprecated("Support for runs is deprecated. This param will have no effect in 1.7.0.", "1.6.0")
  def getRuns: Int = runs

  /**
   * :: Experimental ::
   * Set the number of runs of the algorithm to execute in parallel. We initialize the algorithm
   * this many times with random starting conditions (configured by the initialization mode), then
   * return the best clustering found over any run. Default: 1.
   */
  @deprecated("Support for runs is deprecated. This param will have no effect in 1.7.0.", "1.6.0")
  def setRuns(runs: Int): this.type = {
    if (runs <= 0) {
      throw new IllegalArgumentException("Number of runs must be positive")
    }
    this.runs = runs
    this
  }

  /**
   * Number of steps for the k-means|| initialization mode
   */
  def getInitializationSteps: Int = initializationSteps

  /**
   * Set the number of steps for the k-means|| initialization mode. This is an advanced
   * setting -- the default of 5 is almost always enough. Default: 5.
   */
  def setInitializationSteps(initializationSteps: Int): this.type = {
    if (initializationSteps <= 0) {
      throw new IllegalArgumentException("Number of initialization steps must be positive")
    }
    this.initializationSteps = initializationSteps
    this
  }

  /**
   * The distance threshold within which we've consider centers to have converged.
   */
  def getEpsilon: Double = epsilon

  /**
   * Set the distance threshold within which we've consider centers to have converged.
   * If all centers move less than this Euclidean distance, we stop iterating one run.
   */
  def setEpsilon(epsilon: Double): this.type = {
    this.epsilon = epsilon
    this
  }

  /**
   * The random seed for cluster initialization.
   */
  def getSeed: Long = seed

  /**
   * Set the random seed for cluster initialization.
   */
  def setSeed(seed: Long): this.type = {
    this.seed = seed
    this
  }

  // Initial cluster centers can be provided as a KMeansModel object rather than using the
  // random or k-means|| initializationMode
  private var initialModel: Option[WeightedFuzzyCMeansModel] = None

  /**
   * Set the initial starting point, bypassing the random initialization or k-means||
   * The condition model.k == this.k must be met, failure results
   * in an IllegalArgumentException.
   */
  def setInitialModel(model: WeightedFuzzyCMeansModel): this.type = {
    require(model.k == k, "mismatched cluster count")
    initialModel = Some(model)
    this
  }

  /**
   * Train a Fuzzy C-means model on the given set of points; `data` should be cached for high
   * performance, because this is an iterative algorithm.
   */
  def run(data: RDD[Vector]): WeightedFuzzyCMeansModel = {

    if (data.getStorageLevel == StorageLevel.NONE) {
      logWarning("The input data is not directly cached, which may hurt performance if its"
        + " parent RDDs are also uncached.")

    }

    // Compute squared norms and cache them.
    val norms = data.map(Vectors.norm(_, 2.0))
    norms.persist()
    val zippedData = data.zip(norms).map { case (v, norm) =>
      new VectorWithNorm(v, norm)
    }
    val model = runAlgorithm(zippedData)
    norms.unpersist()

    // Warn at the end of the run as well, for increased visibility.
    if (data.getStorageLevel == StorageLevel.NONE) {
      logWarning("The input data was not directly cached, which may hurt performance if its"
        + " parent RDDs are also uncached.")
    }
    model
  }

  /**
   * Implementation of Weighted Fuzzy C-Means algorithm.
   */
  private def runAlgorithm(data: RDD[VectorWithNorm]): WeightedFuzzyCMeansModel = {

    val sc = data.sparkContext

    val initStartTime = System.nanoTime()

    // Don't care, use any runs
    val numRuns = this.getRuns

    // Calculate centroids with FuzzyCMeans model
    val fcm = FuzzyCMeans.train(data.map(_.vector), k = this.k,
      maxIterations = this.maxIterations,
      runs = this.runs,
      this.initializationMode, seed = this.seed, m = this.m)


    // Set initial centers from FCM
    // the same centers per each run
    val centers = Array.fill(numRuns)(fcm.clusterCenters.map(s => new VectorWithNorm(s)))

    // Beginning centroid weights initialization
    // The same weights per each run
    val initCentroidsWeight = fcm.fuzzyPredict(
      data.map(_.vector))
      .map(point => {
        Vectors.dense(point.map(_._2).toArray)
      }).map(r =>
        r
      ).reduce { (a, b) =>
        axpy(1.0, a, b)
        b
      }.toArray

    var CentroidsWeights = ArrayBuffer.fill(numRuns)(initCentroidsWeight)

    val initTimeInSeconds = (System.nanoTime() - initStartTime) / 1e9
    logInfo(s"Initialization with $initializationMode took " + "%.3f".format(initTimeInSeconds) +
      " seconds.")

    // Initially all runs are active (active == true means the according run has not yet converged)
    // Also Array of Arrays
    val active = Array.fill(numRuns)(true)
    // Initially the costs are 0.0 for all runs
    // Also Array of Arrays
    val costs = Array.fill(numRuns)(0.0)

    // 0, 1, 2, .... nunRuns-1 - ArrayBuffer containing the remaining active runs
    // Initially it contains all the runs
    var activeRuns = new ArrayBuffer[Int] ++ (0 until numRuns)
    var iteration = 0

    val iterationStartTime = System.nanoTime()

    // Stop condition is given by
    // - no more active runs (all runs converged)
    // - maximum number of iterations reached
    // Execute iterations of Lloyd's algorithm until all runs have converged
    while (iteration < maxIterations && activeRuns.nonEmpty) {

      type WeightedPoint = (Vector, Double)

      // this is the function that will be used in the reduce phase
      def mergeContribs(x: WeightedPoint, y: WeightedPoint): WeightedPoint = {
        // y += a * x
        // - in this case y += x
        axpy(1.0, x._1, y._1)
        (y._1, x._2 + y._2)
      }

      // the centers for each run still active
      val activeCenters = activeRuns.map(r => centers(r)).toArray
      // the cost for each run - one accumulator per run
      val costAccums = activeRuns.map(i => {
        val acc = new CostAccumulator(0.0)
        sc.register(acc, f"CostAcc_${i}")
        acc
      })

      // Membership for points.
      // Accumulator per run per cluster
      val weightsUpdateAccumulator = activeRuns.map(i => {
        Array.fill(k) {
          val acc = new MembershipAccumulator(0.0)
          sc.register(acc, f"MembershipAcc_${i}")
          acc
        }
      })

      // broadcast the centers
      val bcActiveCenters = sc.broadcast(activeCenters)

      // broadcast the fuzzifier
      val bcM = sc.broadcast(m)

      // mapPartitions - Return a new RDD by applying a function to each partition of this RDD
      // reduceByKey - Merge the values for each key using an associative reduce function
      // Find the sum and count of points mapping to each center
      val totalContribs = data.zipWithUniqueId.mapPartitions { points =>

        // we're inside the Spark magic now
        // one Array of centers per each active run
        val thisActiveCenters = bcActiveCenters.value
        // how many runs are still active
        val runs = thisActiveCenters.length
        // how many clusters are needed (k)
        val k = thisActiveCenters(0).length
        // the space dimension (number of coordinates)
        val dims = thisActiveCenters(0)(0).vector.size
        // the level of cluster fuzziness
        val m = bcM.value

        // sums are zero (per runs per each dimension)
        val sums = Array.fill(runs, k)(Vectors.zeros(dims))
        // fuzzyCounts are zero  (per run per each dimension)
        val fuzzyCounts = Array.fill(runs, k)(0.0)

        // ++++++++++++++++++++++++++++++++++++++++++++++++++++++
        // Here we assign points to clusters
        // Compute the total cost (sum of distances), sum and count

        points.foreach { pair =>

          val (point, point_id) = pair

          (0 until runs).foreach { i =>
            // WE ARE IN THE CONTEXT OF A SPECIFIC RUN HERE
            val (mbrpDegree, distances) = WeightedFuzzyCMeans.degreesOfMembership(thisActiveCenters(i), point, m)
            // compute membership based cost - ignore "almost zeros"
            mbrpDegree.zipWithIndex.
              filter(_._1 > epsilon * epsilon).
              foreach { degreeWithIndex =>
                val (deg, ind) = degreeWithIndex

                val weight = CentroidsWeights(i)(ind)

                // the total cost increases
                // $J_m(U,v) = \sum \sum w_k (u_k)^m (d_ik)^2$
                // use weights!
                costAccums(i).add(deg * weight * distances(ind))

                // (u_{ij}) * w_j
                weightsUpdateAccumulator(i)(ind).add(weight * deg)
                // add the current point to the  cluster sum
                val sum = sums(i)(ind)
                // use weights!
                // cluster centroid
                axpy(deg * weight, point.vector, sum)
                // increase point count for current cluster
                fuzzyCounts(i)(ind) += deg
              }
          }
        }

        // ++++++++++++++++++++++++++++++++++++++++++++++++++++++

        // For every run and every cluster the sum and count are emitted
        val contribs = for (i <- 0 until runs; j <- 0 until k) yield {
          ((i, j), (sums(i)(j), fuzzyCounts(i)(j)))
        }
        contribs.iterator
        // The key is a combination of run and cluster
        // reduceByKey computes the values across clusters (sum and count)
      }.reduceByKey(mergeContribs).collectAsMap()

      // At this point, for each run, each cluster,
      // we know the sum of vectors and the number of points
      bcActiveCenters.unpersist(blocking = false)

      // Update the cluster centers and costs for each active run
      for ((run, i) <- activeRuns.zipWithIndex) {
        var changed = false
        var j = 0
        // For each cluster
        while (j < k) {
          val (sum, fuzzyCount) = totalContribs((i, j))
          if (fuzzyCount != 0) {
            // x = a * x - multiplies a vector with a scalar
            // Compute new center
            // Include centroid weights in "weight" composition
            val weight = CentroidsWeights(i)(j)
            scal(1.0 / (fuzzyCount * weight), sum)
            val newCenter = new VectorWithNorm(sum)
            // Changed - (distance greater than epsilon squared)
            if (WeightedFuzzyCMeans.fastSquaredDistance(newCenter, centers(run)(j)) > epsilon * epsilon) {
              changed = true
            }
            centers(run)(j) = newCenter
          }
          j += 1
        }
        if (!changed) {
          // Kill the run that converged already
          active(run) = false
          logInfo("Run " + run + " finished in " + (iteration + 1) + " iterations")
        }
        costs(run) = costAccums(i).value
      }

      // remove runs no longer active (active switches to false if there are no changes
      // between 2 successive iterations)
      activeRuns = activeRuns.filter(active(_))

      // update weights
      CentroidsWeights = weightsUpdateAccumulator.map(_.map(_.value))

      // increase number of iterations
      iteration += 1

    }

    val iterationTimeInSeconds = (System.nanoTime() - iterationStartTime) / 1e9
    logInfo(s"Iterations took " + "%.3f".format(iterationTimeInSeconds) + " seconds.")

    if (iteration == maxIterations) {
      logInfo(s"KMeans reached the max number of iterations: $maxIterations.")
    } else {
      logInfo(s"KMeans converged in $iteration iterations.")
    }

    val (minCost, bestRun) = costs.zipWithIndex.min

    logInfo(s"The cost for the best run is $minCost.")

    new WeightedFuzzyCMeansModel(centers(bestRun).map(_.vector), Vectors.dense(CentroidsWeights(bestRun).toArray), m)
  }

  /**
   * Initialize `runs` sets of cluster centers at random.
   */
  private def initRandom(data: RDD[VectorWithNorm])
  : Array[Array[VectorWithNorm]] = {
    // Sample all the cluster centers in one pass to avoid repeated scans
    val sample = data.takeSample(withReplacement = true, runs * k, new XORShiftRandom(this.seed).nextInt()).toSeq
    Array.tabulate(runs)(r => sample.slice(r * k, (r + 1) * k).map { v =>
      new VectorWithNorm(Vectors.dense(v.vector.toArray), v.norm)
    }.toArray)
  }

  /**
   * Initialize `runs` sets of cluster centers using the k-means|| algorithm by Bahmani et al.
   * (Bahmani et al., Scalable K-Means++, VLDB 2012). This is a variant of k-means++ that tries
   * to find with dissimilar cluster centers by starting with a random center and then doing
   * passes where more centers are chosen with probability proportional to their squared distance
   * to the current cluster set. It results in a provable approximation to an optimal clustering.
   *
   * The original paper can be found at http://theory.stanford.edu/~sergei/papers/vldb12-kmpar.pdf.
   */
  private def initKMeansParallel(data: RDD[VectorWithNorm])
  : Array[Array[VectorWithNorm]] = {
    // Initialize empty centers and point costs.
    val centers = Array.tabulate(runs)(r => ArrayBuffer.empty[VectorWithNorm])
    var costs = data.map(_ => Array.fill(runs)(Double.PositiveInfinity))

    // Initialize each run's first center to a random point.
    val seed = new XORShiftRandom(this.seed).nextInt()
    val sample = data.takeSample(withReplacement = true, runs, seed).toSeq
    val newCenters = Array.tabulate(runs)(r => ArrayBuffer(sample(r).toDense))

    /** Merges new centers to centers. */
    def mergeNewCenters(): Unit = {
      var r = 0
      while (r < runs) {
        centers(r) ++= newCenters(r)
        newCenters(r).clear()
        r += 1
      }
    }

    // On each step, sample 2 * k points on average for each run with probability proportional
    // to their squared distance from that run's centers. Note that only distances between points
    // and new centers are computed in each iteration.
    var step = 0
    while (step < initializationSteps) {
      val bcNewCenters = data.context.broadcast(newCenters)
      val preCosts = costs
      costs = data.zip(preCosts).map { case (point, cost) =>
        Array.tabulate(runs) { r =>
          math.min(WeightedFuzzyCMeans.pointCost(bcNewCenters.value(r), point), cost(r))
        }
      }.persist(StorageLevel.MEMORY_AND_DISK)
      val sumCosts = costs
        .aggregate(new Array[Double](runs))(
          seqOp = (s, v) => {
            // s += v
            var r = 0
            while (r < runs) {
              s(r) += v(r)
              r += 1
            }
            s
          },
          combOp = (s0, s1) => {
            // s0 += s1
            var r = 0
            while (r < runs) {
              s0(r) += s1(r)
              r += 1
            }
            s0
          }
        )

      bcNewCenters.unpersist(blocking = false)
      preCosts.unpersist(blocking = false)

      val chosen = data.zip(costs).mapPartitionsWithIndex { (index, pointsWithCosts) =>
        val rand = new XORShiftRandom(seed ^ (step << 16) ^ index)
        pointsWithCosts.flatMap { case (p, c) =>
          val rs = (0 until runs).filter { r =>
            rand.nextDouble() < 2.0 * c(r) * k / sumCosts(r)
          }
          if (rs.nonEmpty) Some(p, rs) else None
        }
      }.collect()
      mergeNewCenters()
      chosen.foreach { case (p, rs) =>
        rs.foreach(newCenters(_) += p.toDense)
      }
      step += 1
    }

    mergeNewCenters()
    costs.unpersist(blocking = false)

    // Finally, we might have a set of more than k candidate centers for each run; weigh each
    // candidate by the number of points in the dataset mapping to it and run a local k-means++
    // on the weighted centers to pick just k of them
    val bcCenters = data.context.broadcast(centers)
    val weightMap = data.flatMap { p =>
      Iterator.tabulate(runs) { r =>
        ((r, WeightedFuzzyCMeans.findClosest(bcCenters.value(r), p)._1), 1.0)
      }
    }.reduceByKey(_ + _).collectAsMap()

    bcCenters.unpersist(blocking = false)

    val finalCenters = (0 until runs).par.map { r =>
      val myCenters = centers(r).toArray
      val myWeights = myCenters.indices.map(i => weightMap.getOrElse((r, i), 0.0)).toArray
      LocalKMeans.kMeansPlusPlus(r, myCenters, myWeights, k, 30)
    }

    finalCenters.toArray
  }
}


/**
 * Top-level methods for calling Weighted Fuzzy C-means clustering.
 */
object WeightedFuzzyCMeans {

  // Initialization mode names
  val RANDOM = "random"
  @Since("0.8.0")
  val K_MEANS_PARALLEL = "k-means||"

  /**
   * Trains a Weighted fuzzy c-means model using the given set of parameters.
   *
   * @param data               training points stored as `RDD[Vector]`
   * @param k                  number of clusters
   * @param maxIterations      max number of iterations
   * @param runs               number of parallel runs, defaults to 1. The best model is returned.
   * @param initializationMode initialization model, either "random" or "k-means||" (default).
   * @param seed               random seed value for cluster initialization
   * @param m                  fuzzyfier, between 1 and infinity, default is 2, 1 leads to hard clustering
   */
  def train(
             data: RDD[Vector],
             k: Int,
             maxIterations: Int,
             runs: Int,
             initializationMode: String,
             seed: Long,
             m: Double): WeightedFuzzyCMeansModel = {
    new WeightedFuzzyCMeans().setK(k)
      .setMaxIterations(maxIterations)
      .setRuns(runs)
      .setInitializationMode(initializationMode)
      .setSeed(seed)
      .setM(m)
      .run(data)
  }

  /**
   * Trains a fuzzy c-means model using the given set of parameters.
   *
   * @param data               training points stored as `RDD[Vector]`
   * @param k                  number of clusters
   * @param maxIterations      max number of iterations
   * @param runs               number of parallel runs, defaults to 1. The best model is returned.
   * @param initializationMode initialization model, either "random" or "k-means||" (default).
   * @param seed               random seed value for cluster initialization
   */
  def train(
             data: RDD[Vector],
             k: Int,
             maxIterations: Int,
             runs: Int,
             initializationMode: String,
             seed: Long): WeightedFuzzyCMeansModel = {
    new WeightedFuzzyCMeans().setK(k)
      .setMaxIterations(maxIterations)
      .setRuns(runs)
      .setInitializationMode(initializationMode)
      .setSeed(seed)
      .run(data)
  }

  /**
   * Trains a fuzzy c-means model using the given set of parameters.
   *
   * @param data               training points stored as `RDD[Vector]`
   * @param k                  number of clusters
   * @param maxIterations      max number of iterations
   * @param runs               number of parallel runs, defaults to 1. The best model is returned.
   * @param initializationMode initialization model, either "random" or "k-means||" (default).
   */
  def train(
             data: RDD[Vector],
             k: Int,
             maxIterations: Int,
             runs: Int,
             initializationMode: String): WeightedFuzzyCMeansModel = {
    new WeightedFuzzyCMeans().setK(k)
      .setMaxIterations(maxIterations)
      .setRuns(runs)
      .setInitializationMode(initializationMode)
      .run(data)
  }

  /**
   * Trains a fuzzy c-means model using specified parameters and the default values for unspecified.
   */
  def train(
             data: RDD[Vector],
             k: Int,
             maxIterations: Int): WeightedFuzzyCMeansModel = {
    train(data, k, maxIterations, 1, K_MEANS_PARALLEL)
  }

  /**
   * Trains a fuzzy c-means model using specified parameters and the default values for unspecified.
   *
   * @param data          training points stored as `RDD[Vector]`
   * @param k             number of clusters
   * @param maxIterations max number of iterations
   * @param m             fuzzyfier, between 1 and infinity, default is 2, 1 leads to hard clustering
   */
  def train(
             data: RDD[Vector],
             k: Int,
             m: Double,
             maxIterations: Int
           ): WeightedFuzzyCMeansModel = {
    new WeightedFuzzyCMeans().setK(k)
      .setMaxIterations(maxIterations)
      .setK(k)
      .setM(m)
      .run(data)
  }

  /**
   * Trains a k-means model using specified parameters and the default values for unspecified.
   */
  def train(
             data: RDD[Vector],
             k: Int,
             maxIterations: Int,
             runs: Int): WeightedFuzzyCMeansModel = {
    train(data, k, maxIterations, runs, K_MEANS_PARALLEL)
  }

  /**
   * Returns the index of the closest center to the given point, as well as the squared distance.
   */
  private[mllib] def findClosest(
                                  centers: TraversableOnce[VectorWithNorm],
                                  point: VectorWithNorm): (Int, Double) = {
    var bestDistance = Double.PositiveInfinity
    var bestIndex = 0
    var i = 0
    centers.foreach { center =>
      // Since `\|a - b\| \geq |\|a\| - \|b\||`, we can use this lower bound to avoid unnecessary
      // distance computation.
      var lowerBoundOfSqDist = center.norm - point.norm
      lowerBoundOfSqDist = lowerBoundOfSqDist * lowerBoundOfSqDist
      if (lowerBoundOfSqDist < bestDistance) {
        val distance: Double = fastSquaredDistance(center, point)
        if (distance < bestDistance) {
          bestDistance = distance
          bestIndex = i
        }
      }
      i += 1
    }
    (bestIndex, bestDistance)
  }

  /**
   * Returns the degree of membership of the point to each of the clusters
   * Along with the array of distances from the point to each centroid
   */
  private[mllib] def degreesOfMembership(
                                          centers: Array[VectorWithNorm],
                                          point: VectorWithNorm,
                                          fuzzifier: Double): (Array[Double], Array[Double]) = {

    if (fuzzifier == 1) {

      // This is classical hard clustering
      val (bestIndex, bestDistance) = findClosest(centers, point)
      val distances = Array.fill(centers.length)(0.0)
      val membershipDegrees = distances
      distances(bestIndex) = bestDistance
      membershipDegrees(bestIndex) = 1
      (membershipDegrees, distances)

    } else {

      // Distances from the point to each centroid
      val distances = centers map (fastSquaredDistance(_, point))

      val perfectMatches = distances.count(d => d == 0.0)
      if (perfectMatches > 0) {
        // If at least one of the distances is 0 the membership divides between
        // the perfect matches
        (distances map (d => if (d == 0.0) 1.0 / perfectMatches else 0.0), distances)
      } else {
        // Standard formula
        // $w_{ij} = \frac{1}{\sum...}

        // pow = \frac{2}{m-1}
        val pow = 2.0 / (fuzzifier - 1.0)
        // d = \sum{k=1}{c}(\frac{||x_i - c_j||}{||x_i - c_l||})
        val denom = distances.foldLeft(0.0)((sum, dik) => sum + Math.pow(1 / dik, pow))

        // $w_{ij} = \frac{1}{d^pow}$
        (distances map (dij => 1 / (Math.pow(dij, pow) * denom)), distances)
      }
    }
  }


  /**
   * Returns the cost of a given point against the given cluster centers.
   */
  private[mllib] def pointCost(
                                centers: TraversableOnce[VectorWithNorm],
                                point: VectorWithNorm): Double =
    findClosest(centers, point)._2

  /**
   * Returns the squared Euclidean distance between two vectors computed by
   * [[org.apache.spark.mllib.util.MLUtils#fastSquaredDistance]].
   */
  private[clustering] def fastSquaredDistance(
                                               v1: VectorWithNorm,
                                               v2: VectorWithNorm): Double = {
    MLUtils.fastSquaredDistance(v1.vector, v1.norm, v2.vector, v2.norm)
  }

  private[spark] def validateInitMode(initMode: String): Boolean = {
    initMode match {
      case KMeans.RANDOM => true
      case KMeans.K_MEANS_PARALLEL => true
      case _ => false
    }
  }
}


private[clustering]
class MembershipAccumulator(private var sum: Double = 0) extends AccumulatorV2[Double, Double] {

  override def isZero: Boolean = sum == 0

  override def copy(): AccumulatorV2[Double, Double] = {

    val newMembershipAcc = new MembershipAccumulator()

    newMembershipAcc.sum = this.sum
    newMembershipAcc
  }

  override def reset(): Unit = sum = 0

  override def add(v: Double): Unit = sum += v

  override def merge(other: AccumulatorV2[Double, Double]): Unit = sum += other.value

  override def value: Double = sum
}