package org.apache.spark.mllib.clustering.runners

import org.apache.spark.mllib.clustering.UMicro
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object UMicroRunner {

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
      Vectors.dense(9.0, 0.1),
      Vectors.dense(9.1, 0.0),
      Vectors.dense(0.0,9.0),
      Vectors.dense(0.1,9.0),
      Vectors.dense(0.0,9.1),
      Vectors.dense(9.0,9.0),
      Vectors.dense(9.0,9.1),
      Vectors.dense(9.1,9.0)
    )

    val rdd = sc.parallelize(points, 3)

    val pre_model = UMicro.train(rdd,4)

    val model = UMicro.train(rdd,2, pre_model.uncertainClusters)

    val predicts = model.predict(rdd).collect()

    rdd.collect().zipWithIndex zip predicts foreach { predict =>
      println(s"${predict._1._2} Point ${predict._1._1} is in ${predict._2}")

    }

  }

}
