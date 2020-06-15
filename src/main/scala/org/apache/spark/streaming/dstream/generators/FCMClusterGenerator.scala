package org.apache.spark.streaming.dstream.generators

import org.apache.spark.internal.Logging
import org.apache.spark.mllib.clustering.FuzzyCMeans
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.sources.StreamSource

class FCMClusterGenerator(override val source: StreamSource[Vector],
                          override val session: SparkSession,
                          val window: Int = 60
                       ) extends StreamGenerator[Vector](source,session) with Logging {

  val k = 3
  val m = 1
  val maxIterations = 1000

  /**
   * Blocking function that start streaming
   */
  override def run(): Unit = {

    val ss = session

    this.source.sliding(window).foreach(chunk => {

      logInfo(s"Reached ${chunk.length} new records. Writing to Kafka.")

      val rdd = ss.sparkContext.parallelize(chunk.map(x => Vectors.dense(x.toArray)))

      val nextIter = FuzzyCMeans.train(rdd,k,maxIterations)

      val fuzzyPredicts = nextIter.fuzzyPredict(rdd).collect()

      rdd.collect() zip fuzzyPredicts foreach { fuzzyPredict =>
        println(s" Point ${fuzzyPredict._1}")
        fuzzyPredict._2 foreach{clusterAndProbability =>
          println(s"Probability to belong to cluster ${clusterAndProbability._1} " +
            s"is ${"%.2f".format(clusterAndProbability._2)}")
        }
      }


      logInfo(s"Sleep for ${timeout}")
      Thread.sleep(timeout)
    })}

}
