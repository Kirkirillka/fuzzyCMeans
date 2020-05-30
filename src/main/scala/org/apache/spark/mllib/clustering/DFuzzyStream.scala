package org.apache.spark.mllib.clustering

import java.util.{Calendar, Date}

import org.apache.spark.internal.Logging
import org.apache.spark.mllib.clustering
import org.apache.spark.mllib.linalg.BLAS.{axpy, scal}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer


class DFuzzyStream private(
                    var min_fmics: Int,
                    var max_fmics: Int,
                    var threshold: Double,
                    var m: Double
                  ) extends Serializable with Logging {


  require(max_fmics >= min_fmics)

  def this() = this(3,5,1e-4, 2.0)


  def setM(M: Double) = {
    m = M
    this
  }

  private var FmiC = ArrayBuffer.empty[FuzzyCluster]

  private def remoteOldest(): ArrayBuffer[FuzzyCluster] = {
    FmiC.synchronized {

      val oldestTimeAccess = FmiC.map(_.getTime).min

      FmiC.filter(_.getTime == oldestTimeAccess)
    }
  }

  def run(data: RDD[Vector]): DFuzzyStreamModel = {

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


  def runAlgorithm(data: RDD[VectorWithNorm]): DFuzzyStreamModel = {

    data.foreach(point => {

      // Check if we have enough number of clusters
      // If less than minimum, then add a new point as cluster
      if (FmiC.length < min_fmics) {
        val newFmiC = new FuzzyCluster(point)
        FmiC.append(newFmiC)
      }
      else {

        //val distances = FmiC.map(pair => DFuzzyStream.fastSquaredDistance(pair._1.c, point))

        val (mbrpDegree, distances) = WeightedFuzzyCMeans.degreesOfMembership(FmiC.map(_.c).toArray, point, m)

        var isOutlier = true

        FmiC.zipWithIndex.foreach(pair => {

          val (cluster, idx) = pair

          val radius = cluster match {
            case cluster if cluster.getN == 1 => FmiC.filter(_ != cluster)
              .map(cluster => DFuzzyStream.fastSquaredDistance(cluster.c, point)).min
            case _ => cluster.dp
          }

          if (distances(idx) <= radius) {
            isOutlier = false
            // Update access time
            cluster.setTime(Calendar.getInstance.getTime)
          }
        })

        // If an outlier is detected
        if (isOutlier) {
          // If we reached the limit of clusters
          if (FmiC.length >= max_fmics) {
            // Delete old fuzzy clusters
            FmiC = remoteOldest()
          }
          // It defines a new fuzzy cluster
          val newCluster = new FuzzyCluster(point)
          FmiC.append(newCluster)
        }
        // If inside one of the existing fuzzy clusters
        else {

          // update existing tagets
          FmiC = FmiC.zipWithIndex.map(pair => {

            val (cluster, idx) = pair

            val tgDistance = distances(idx)
            val tgMembership = mbrpDegree(idx)

            cluster.update(point, tgDistance, tgMembership)
          })

        }

      }
    })

    new DFuzzyStreamModel(
      FmiC.toArray
    )
  }

}

object DFuzzyStream {

  /**
   * Trains a d-FuzzyStream model using the given set of parameters.
   *
   * @param data               training points stored as `RDD[Vector]`
   * @param m                  fuzzyfier, between 1 and infinity, default is 2, 1 leads to hard clustering
   */
  def train(
             data: RDD[Vector],
             m: Double): DFuzzyStreamModel = {
    new clustering.DFuzzyStream()
      .setM(m)
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



class FuzzyCluster(
                    private var cf: VectorWithNorm,
                    private var ssd: Double,
                    private var N: Long,
                    private var t: Date,
                    private var M: Double
                  ) {

  def this(point: VectorWithNorm) = this(point, 0, 1, Calendar.getInstance.getTime, 1.0)

  def getN = N

  def getTime = t

  def setTime(newTime: Date) = {
    t = newTime
    this
  }

  private def updateCF(point: VectorWithNorm, membership: Double) = {
    axpy(membership, point.vector, cf.vector)
    new VectorWithNorm(cf.vector)
  }

  private def updateSSD(distance: Double, membership: Double) = {
    ssd + (membership * Math.sqrt(distance))
  }


  def update(point: VectorWithNorm, distance: Double, membership: Double) = {
    ssd = updateSSD(distance, membership)
    cf = updateCF(point, membership)
    N += 1
    M += membership

    this
  }

  def c = {
    scal(1 / M, cf.vector)
    new VectorWithNorm(cf.vector)
  }

  def dp = Math.sqrt(ssd / N)

  def FR(that: FuzzyCluster) = (this.dp + that.dp) / (DFuzzyStream.fastSquaredDistance(this.c, that.c))
}