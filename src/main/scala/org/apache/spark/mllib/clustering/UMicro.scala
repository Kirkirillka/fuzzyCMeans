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

  def setN(value: Int): this.type = {
    require(value >= 2)
    nMicro = value
    this
  }

  def getN = nMicro

  def getThreshold = threshold

  def setThreshold(value: Double): this.type = {
    require(value > 0)
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
            cluster.touch()
            acc.append(cluster)
          }
          else {

            val pairs = acc.map(r => (r, r.expectedDistance(cluster)))

            val (minCluster, minDistance) = pairs.minBy(_._2)

            val expectedSimilarity = minCluster.expectedSimilarity(cluster)

            // if inside critical uncertain border
            if ( expectedSimilarity < threshold) {
              minCluster.addPoint(cluster)
            }
            else {
              // Set current time
              cluster.touch()
              acc.append(cluster)
            }

            // remove the least recently updated clusers
            if (acc.length >= nMicro + 1) {
              val oldestTimestamp = acc.map(_.getTime).min
              acc --= acc.filter(_.getTime == oldestTimestamp)
            }
          }
        })

        acc

      })

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
   */
  def train(
             data: RDD[Vector]): UMicroModel = {
    new clustering.UMicro()
      .run(data)
  }

  /**
   * Trains a d-FuzzyStream model using the given set of parameters.
   *
   * @param data training points stored as `RDD[Vector]`
   * @n number of uncertain cluster to produce
   */
  def train(
             data: RDD[Vector],
             n: Int): UMicroModel = {
    new clustering.UMicro()
      .setN(n)
      .run(data)
  }


  /**
   * Trains a d-FuzzyStream model using the given set of parameters.
   *
   * @param data training points stored as `RDD[Vector]`
   * @n number of uncertain cluster to produce
   * @threshold critical uncertain boundary value
   */
  def train(
             data: RDD[Vector],
             n: Int,
             threshold: Double): UMicroModel = {
    new clustering.UMicro()
      .setN(n)
      .setThreshold(threshold)
      .run(data)
  }

  /**
   * Trains a d-FuzzyStream model using the given set of parameters.
   *
   * @param data training points stored as `RDD[Vector]`
   * @n number of uncertain cluster to produce
   * @initialModel Initial uncertain clusters to use
   */
  def train(
             data: RDD[Vector],
             n: Int,
             initialModel: Array[UncertainCluster]): UMicroModel = {
    new clustering.UMicro()
      .setN(n)
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
 * @param cf1 sum of data points over each d-dimensions, norm-1 vector, access to to real values
 *            through CF1 and CF2  function
 * @param N   number of data points in uncertain cluster
 * @param t   time of last updates
 */
case class UncertainCluster(private var cf2: Vector,
                            private var ef2: Vector,
                            private var cf1: Vector,
                            private var N: Long,
                            private var t: Date) extends Serializable {

  def this(point: Vector) =
    this(
      Vectors.dense(point.toArray.map(Math.pow(_, 2.0))),
      Vectors.dense(Array.fill(point.size)(0.0)),
      point.copy,
      1L,
      Calendar.getInstance.getTime)

  def getN = N

  def getTime = t

  def setTime(newTime: Date) = {
    t = newTime
    this
  }

  private def sumVec(vec: Vector) = vec.toArray.sum

  private def powVec(vec: Vector, level: Double = 2.0) = Vectors.dense(vec.toArray.map(Math.pow(_, level)))

  def addPoint(point: Vector): Unit = {
    touch()
    axpy(1.0, point, cf1)
    axpy(1.0, powVec(point), cf2)
    axpy(1.0, powVec(getErrorVector(point)), ef2)
    N += 1
  }

  def addPoint(cluster: UncertainCluster): Unit = addPoint(cluster.expectedClusterCenter)

  def expectedClusterCenter = {

    val center = Vectors.dense(Array.fill(cf1.size)(0.0))

    val cf1_loc = cf1.copy
    val ef1_loc = powVec(ef2.copy, 0.5)

    axpy(1 / N, cf1_loc, center)
    axpy(1 / N, ef1_loc, center)

    center
  }

  def expectedSimilarity(dataPoint: Vector) = {

    // Lemma 2.1
    sumVec(cf2) / Math.pow(N, 2.0) + sumVec(powVec(getErrorVector(dataPoint))) / Math.pow(N, 2.0)
  }

  def expectedSimilarity(cluster: UncertainCluster): Double = expectedSimilarity(cluster.expectedClusterCenter)

  def touch() = setTime(Calendar.getInstance.getTime)

  def expectedDistance(cluster: UncertainCluster): Double = expectedDistance(cluster.expectedClusterCenter)

  def expectedDistance(dataPoint: Vector) = {

    val error_loc = getErrorVector(dataPoint)

    val interm = {
      val a = dataPoint.toArray
      val b = cf1.toArray

      (a.zip(b).map(pair => pair._1 * pair._2)).sum
    }

    // Lemma 2.2
    expectedSimilarity(dataPoint)+ sumVec(powVec(dataPoint)) + sumVec(powVec(error_loc)) - 2 * interm / N

  }

  private def getErrorVector(dataPoint: Vector) = {
    val errorVector = cf1.copy

    axpy(-1.0, dataPoint, errorVector)

    errorVector

  }

}


case class nMicroAccumulator(var count: Int = 0) extends AccumulatorV2[Int, Int] {
  override def isZero: Boolean = count == 0

  override def copy(): AccumulatorV2[Int, Int] = new nMicroAccumulator(count)

  override def reset(): Unit = count = 0

  override def add(v: Int): Unit = count += 1

  override def merge(other: AccumulatorV2[Int, Int]): Unit = count += other.value

  override def value: Int = count
}
