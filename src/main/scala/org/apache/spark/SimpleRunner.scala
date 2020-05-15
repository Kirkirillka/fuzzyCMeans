package org.apache.spark

import org.apache.spark.mllib.clustering.FuzzyCMeans
import org.apache.spark.mllib.clustering.KMeans.{K_MEANS_PARALLEL, RANDOM}
import org.apache.spark.mllib.linalg.Vectors

object SimpleRunner {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("MLlibUnitTest")
    val sc = new SparkContext(conf)


    val points = Seq(
      Vectors.dense(0.0, 0.0, 3),
      Vectors.dense(0.0, 0.1,4),
      Vectors.dense(0.1, 0.0,5),
      Vectors.dense(9.0, 0.0,1),
      Vectors.dense(9.0, 0.2,2),
      Vectors.dense(9.2, 0.0,7)
    )
    val rdd = sc.parallelize(points, 3).cache()

    for (initMode <- Seq(RANDOM, K_MEANS_PARALLEL)) {

      (1 to 4).map(_ * 2) foreach { fuzzifier =>

        val model = FuzzyCMeans.train(rdd, k = 3, maxIterations = 1000, runs = 100, initMode,
          seed = 26031979L, m = fuzzifier)

        val fuzzyPredicts = model.fuzzyPredict(rdd).collect()

        rdd.collect() zip fuzzyPredicts foreach { fuzzyPredict =>
          println(s" Point ${fuzzyPredict._1}")
          fuzzyPredict._2 foreach{clusterAndProbability =>
            println(s"Probability to belong to cluster ${clusterAndProbability._1} " +
              s"is ${"%.2f".format(clusterAndProbability._2)}")
          }
        }
      }
    }

  }

}
