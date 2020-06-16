package org.apache.spark.streaming.adapters

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.generators.Utils.{JDBCParams, KafkaParams}
import org.apache.spark.streaming.adapters.Transform.castToTimestampVector

object Outputs {

  def _toPostgresSQL(params: JDBCParams) = (data: RDD[Vector]) => {

    val ss = SparkSession.builder().sparkContext(data.sparkContext).getOrCreate()
    import ss.implicits._

    castToTimestampVector(data)
      // use case class ClusterRecord to add user-friendly names for columns being saved
      .toDS()
      .write
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", s"jdbc:postgresql://${params("server")}/")
      .option("user", params("user"))
      .option("password", params("password"))
      .option("dbtable", s"${params("dbtable")}")
      // SaveMode.Append important for streaming batches, Spark checks if table exists.
      .mode(SaveMode.Append)
      .save()

    data
  }


  def _toKafka(params: KafkaParams) = (data: RDD[Vector]) => {

    val session = SparkSession.builder().sparkContext(data.sparkContext).getOrCreate()
    import session.implicits._

    data.map(_.toArray).toDS
      .write
      .format("kafka")
      .option("topic", params("inTopics"))
      .option("kafka.bootstrap.servers", params("servers"))
      .save()

    data
  }


  def _toConsole() = (data: RDD[Vector]) => {

    val session = SparkSession.builder().sparkContext(data.sparkContext).getOrCreate()
    import session.implicits._

    data.map(_.toArray).toDS
      .write
      .format("console")
      .save()

    data
  }


  def toFuzzyClusterPrint(data: RDD[Vector],
                               fuzzyPredicts:  RDD[Seq[(Int, Double)]] ) = {

    data zip fuzzyPredicts foreach { fuzzyPredict =>
      println(s" Point ${fuzzyPredict._1}")
      fuzzyPredict._2 foreach{clusterAndProbability =>
        println(s"Probability to belong to cluster ${clusterAndProbability._1} " +
          s"is ${"%.2f".format(clusterAndProbability._2)}")
      }
    }

    data
  }

  def toClusterPrint(data: RDD[Vector],
                          predicts:  RDD[Int] ) = {

    data.zipWithIndex zip predicts foreach { predict =>
      println(s"${predict._1._2} Point ${predict._1._1} is in ${predict._2}")

    }

    data
  }






  def fuzzyResultSaveToPostgreSQL(data: RDD[Vector],
                          fuzzyPredicts:  RDD[Seq[(Int, Double)]] , dataSinkParams: JDBCParams,
                                  clusterSinkParams: JDBCParams) = {




    data zip fuzzyPredicts foreach { fuzzyPredict =>
      println(s" Point ${fuzzyPredict._1}")
      fuzzyPredict._2 foreach{clusterAndProbability =>
        println(s"Probability to belong to cluster ${clusterAndProbability._1} " +
          s"is ${"%.2f".format(clusterAndProbability._2)}")
      }
    }

    data
  }

}
