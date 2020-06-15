package org.apache.spark.mllib.clustering.runners

import org.apache.spark.mllib.clustering.StreamingFuzzyCMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object SFCMSimpleRunner {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SFCM_MLlibUnitTest")
    val sc = new SparkContext(conf)


    val points = Seq(
      Vectors.dense(5.0, 9.0),
      Vectors.dense(5.0, 0.1),
      Vectors.dense(5.1, 3.0),
      Vectors.dense(9.0, 0.0),
      Vectors.dense(9.0, 5.2),
      Vectors.dense(9.2, 0.0)
    )
    val rdd = sc.parallelize(points, 3).cache()
    rdd.cache()

    var model = StreamingFuzzyCMeans.train(rdd, k = 3 , maxIterations = 10)

    val histModels = ArrayBuffer(model)

    for {_ <- 1 to 30} {
      val iterative = StreamingFuzzyCMeans.train(rdd, k = 3, maxIterations = 200,histModels.takeRight(1))

      histModels.append(iterative)

      model = iterative

      val fuzzyPredicts = model.fuzzyPredict(rdd).collect()

      rdd.collect() zip fuzzyPredicts foreach { fuzzyPredict =>
        println(s" Point ${fuzzyPredict._1}")
        fuzzyPredict._2 foreach { clusterAndProbability =>
          println(s"Probability to belong to cluster ${clusterAndProbability._1} " +
            s"is ${"%.2f".format(clusterAndProbability._2)}")
        }
      }

    }



  }

}
