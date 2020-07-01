package org.apache.spark.streaming.adapters

import java.time.LocalDateTime

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.adapters.Transform._
import org.apache.spark.streaming.dstream.generators.Utils.{JDBCParams, KafkaParams}

object Outputs {

  def _toPostgresSQL(params: JDBCParams,optional_tb_prefix: Option[String] = None) = (data: RDD[Vector], timestamp: LocalDateTime) => {

    val ss = SparkSession.builder().sparkContext(data.sparkContext).getOrCreate()
    import ss.implicits._

    casttoDataPointRecord(data, Some(timestamp))
      // use case class ClusterRecord to add user-friendly names for columns being saved
      .toDS()
      .write
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", s"jdbc:postgresql://${params("server")}/")
      .option("user", params("user"))
      .option("password", params("password"))
      .option("dbtable", optional_tb_prefix match {
        case Some(a) => s"${a}_${params("dbtable")}"
        case None => s"${params("dbtable")}"
      })
      // SaveMode.Append important for streaming batches, Spark checks if table exists.
      .mode(SaveMode.Append)
      .save()

    (data, timestamp)
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



  def toFuzzyPredictionResultSaveToPostgreSQL(params: JDBCParams, optional_tb_prefix: Option[String] = None) = (data: RDD[Vector],
                                                                     fuzzyPredicts:  RDD[Seq[(Int, Double)]], timestamp: LocalDateTime) => {


    val ss = SparkSession.builder().sparkContext(data.sparkContext).getOrCreate()
    import ss.implicits._

    castToFuzzyPredictionRecord(data, fuzzyPredicts, Some(timestamp))
      // use case class DataPointFuzzyResult to add user-friendly names for columns being saved
      .toDS()
      .write
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", s"jdbc:postgresql://${params("server")}/")
      .option("user", params("user"))
      .option("password", params("password"))
      .option("dbtable", optional_tb_prefix match {
        case Some(a) => s"${a}_${params("dbtable")}"
        case None => s"${params("dbtable")}"
      })
      // SaveMode.Append important for streaming batches, Spark checks if table exists.
      .mode(SaveMode.Append)
      .save()

    (data, fuzzyPredicts)

  }


  def toCrispPredictionResultSaveToPostgreSQL(params: JDBCParams,  optional_tb_prefix: Option[String] = None) = (data: RDD[Vector],
                                                                     fuzzyPredicts:  RDD[Int],
                                                                     timestamp: LocalDateTime) => {


    val ss = SparkSession.builder().sparkContext(data.sparkContext).getOrCreate()
    import ss.implicits._

    castToCrispPredictionRecord(data, fuzzyPredicts, Some(timestamp))
      // use case class DataPointFuzzyResult to add user-friendly names for columns being saved
      .toDS()
      .write
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", s"jdbc:postgresql://${params("server")}/")
      .option("user", params("user"))
      .option("password", params("password"))
      .option("dbtable", optional_tb_prefix match {
        case Some(a) => s"${a}_${params("dbtable")}"
        case None => s"${params("dbtable")}"
      })
      // SaveMode.Append important for streaming batches, Spark checks if table exists.
      .mode(SaveMode.Append)
      .save()

    (data, fuzzyPredicts)

  }

}
