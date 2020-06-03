package org.apache.spark.mllib.clustering

import java.util.{Calendar, Date}

import org.apache.spark.internal.Logging
import org.apache.spark.mllib.{clustering, linalg}
import org.apache.spark.mllib.linalg.BLAS.{axpy, dot, scal}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.ArrayBuffer


class UMicro private(
                      private var nMicro: Int,
                      private var threshold: Double
                    ) extends Serializable with Logging {


  // standard deviation = 3
  def this() = this(2, 3.0)


  def setThreshold(value: Double): this.type = {
    require(value >= 0)
    threshold = value
    this
  }

  private var initialModel: Option[ArrayBuffer[UncertainCluster]] = None

  /**
   * Set the initial starting point, bypassing the initial emtpy ArrayBuffer for fold operation
   */
  def setInitialModel(model: ArrayBuffer[UncertainCluster]): this.type = {
    initialModel = Some(model)
    this
  }

  private def remoteOldest(clusters: ArrayBuffer[UncertainCluster]): ArrayBuffer[UncertainCluster] = {
    clusters.synchronized {

      val oldestTimeAccess = clusters.map(_.getTime).min

      clusters.filter(_.getTime == oldestTimeAccess)
    }
  }

  def run(data: RDD[Vector]): UMicroModel = {

    if (data.getStorageLevel == StorageLevel.NONE) {
      logWarning("The input data is not directly cached, which may hurt performance if its"
        + " parent RDDs are also uncached.")
    }

    val model = runAlgorithm(data)

    // Warn at the end of the run as well, for increased visibility.
    if (data.getStorageLevel == StorageLevel.NONE) {
      logWarning("The input data was not directly cached, which may hurt performance if its"
        + " parent RDDs are also uncached.")
    }
    model
  }


  def runAlgorithm(data: RDD[Vector]): UMicroModel = {

    val sc = data.sparkContext

    val initStartTime = System.nanoTime()

    val initCluster = initialModel match {
      case Some(model) => model
      case None => ArrayBuffer.empty[UncertainCluster]
    }

    val initTimeInSeconds = (System.nanoTime() - initStartTime) / 1e9
    logInfo(s"Initialization took" + "%.3f".format(initTimeInSeconds) +
      " seconds.")

    // HAD to use .collect() to execute on driver
    // because it actively uses a array updates

    val finalFMICs = data.map(new UncertainCluster(_))
      .map(ArrayBuffer(_))
      .fold(initCluster)((acc, clusters) => {

        clusters.foreach(cluster => {

          if (acc.length < nMicro) {
            acc.append(cluster)
          }
          else {

            val (minCluster, minDistance) = acc.map(r => (r, r.distance(cluster))).minBy(_._2)



          }

        acc

      })

    println(finalFMICs)

    new UMicroModel(
      finalFMICs.toArray
    )
  }

}

object UMicro {

  /**
   * Trains a d-FuzzyStream model using the given set of parameters.
   *
   * @param data training points stored as `RDD[Vector]`
   * @param m    fuzzyfier, between 1 and infinity, default is 2, 1 leads to hard clustering
   */
  def train(
             data: RDD[Vector],
             m: Double): UMicroModel = {
    new clustering.UMicro()
      .run(data)
  }

  /**
   * Trains a d-FuzzyStream model using the given set of parameters.
   *
   * @param data training points stored as `RDD[Vector]`
   * @param m    fuzzyfier, between 1 and infinity, default is 2, 1 leads to hard clustering
   */
  def train(
             data: RDD[Vector],
             m: Double,
             initialModel: Array[UncertainCluster]): UMicroModel = {
    new clustering.UMicro()
      .setInitialModel(initialModel.to[ArrayBuffer])
      .run(data)
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
   * Returns the squared Euclidean distance between two vectors computed by
   * [[org.apache.spark.mllib.util.MLUtils#fastSquaredDistance]].
   */
  private[clustering] def fastSquaredDistance(
                                               v1: VectorWithNorm,
                                               v2: VectorWithNorm): Double = {
    MLUtils.fastSquaredDistance(v1.vector, v1.norm, v2.vector, v2.norm)
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


}

/**
 *
 * @param ef  sum of squares of errors, actually, is stored as norm-1 vector, access to real ef through EF func
 * @param cf1 sum of data points over each d-dimensions, norm-1 vector, access to to real values
 *            through CF1 and CF2  function
 * @param N   number of data points in uncertain cluster
 * @param t   time of last updates
 */
case class UncertainCluster(private var cf2: Vector,
                            private var ef1: Vector,
                            private var cf1: Vector,
                            private var N: Long,
                            private var t: Date,
                           ) extends Serializable {

  def this(point: Vector) = this(Vectors.dense(point.toArray.map(Math.pow(_,2.0))),
    Vectors.dense(Array.fill(point.size)(0.0)), point.copy, 1, Calendar.getInstance.getTime)

  def getN = N

  def getTime = t

  def setTime(newTime: Date) = {
    t = newTime
    this
  }

  private def pow2(vec: Vector) = Vectors.dense(vec.toArray.map(Math.pow(_,2.0)))

  def ef2 = pow2(ef1)

  def expectedClusterCenter = {
    val centroid = cf1.copy
    val copyEr = ef1.copy

    scal(1 / N, centroid)
    scal(1 / N, copyEr)

    axpy(1.0, copyEr, centroid)

    centroid
  }

  def uncertainRadius(dataPoint: Vector) = distance(dataPoint)

  def uncertainRadius(cluster: UncertainCluster) = distance(cluster.expectedClusterCenter)

  def distance(cluster: UncertainCluster) = distance(cluster.expectedClusterCenter)

  def distance(dataPoint: Vector) = {

    val distance = Vectors.dense(Array.fill(dataPoint.size)(0.0))

    val cf1_loc = cf1.copy
    val ef2_loc = ef2.copy
    val error_loc = getErrorVector(dataPoint)


    // Expected value term
    axpy(1/Math.pow(N,2.0),pow2(cf1_loc), distance)

    axpy(1/Math.pow(N,2.0),pow2(ef2_loc), distance)

    axpy(1.0,pow2(dataPoint), distance)

    axpy(1.0,pow2(error_loc), distance)

    val interm = {
      val a = dataPoint.toArray
      val b = cf1.toArray

      Vectors.dense(a.zip(b).map(pair => pair._1 * pair._2))
    }

    axpy(-2.0/N,interm, distance)

    // sum
    Vectors.norm(distance, 1.0)

  }

  private def getErrorVector(dataPoint: Vector) = {
    val errorVector = expectedClusterCenter

    axpy(-1.0, dataPoint, errorVector)

    errorVector

  }


}