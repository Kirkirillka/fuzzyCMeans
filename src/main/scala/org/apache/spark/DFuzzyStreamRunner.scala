package org.apache.spark

import org.apache.spark.mllib.clustering.DFuzzyStream
import org.apache.spark.mllib.linalg.Vectors

object DFuzzyStreamRunner {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("MLlibUnitTest")
    val sc = new SparkContext(conf)


    val points = Seq(
      Vectors.dense(0.0, 0.0),
      Vectors.dense(0.0, 0.1),
      Vectors.dense(0.1, 0.0),
      Vectors.dense(9.0, 0.0),
      Vectors.dense(9.0, 0.2),
      Vectors.dense(9.2, 0.0),
      Vectors.dense(5.0, 9.0),
      Vectors.dense(5.0, 0.1),
      Vectors.dense(5.1, 3.0),
      Vectors.dense(9.0, 0.0),
      Vectors.dense(9.0, 5.2),
      Vectors.dense(9.2, 0.0),
      Vectors.dense(1,2),
        Vectors.dense(1,3)
    )
    val rdd = sc.parallelize(points, 3).cache()

    val rdd2 = sc.parallelize(points, 3).cache()

    val model = DFuzzyStream.train(rdd, 2.0)

    val fuzzyPredicts = model.fuzzyPredict(rdd2).collect()

    assert(points.length == fuzzyPredicts.length)

    rdd.collect() zip fuzzyPredicts foreach { fuzzyPredict =>
      println(s" Point ${fuzzyPredict._1}")
      fuzzyPredict._2 foreach { clusterAndProbability =>
        println(s"Probability to belong to cluster ${clusterAndProbability._1} " +
          s"is ${"%.2f".format(clusterAndProbability._2)}")
      }
    }

  }

}
