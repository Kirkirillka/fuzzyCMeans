package org.apache.spark.streaming.adapters

import java.time.LocalDateTime

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.adapters.Models.{DataPointRecord, CrispPredictionRecord, FuzzyPredictionRecord}

object Transform {

  def casttoDataPointRecord(data: RDD[Vector], accessTime: Option[LocalDateTime] = None) = {

    val timeArray = accessTime match {
      case Some(arr) => arr
      case None => LocalDateTime.now()
    }

    data zipWithUniqueId() map (x => DataPointRecord(timeArray.toString, x._1.toArray, x._2))

  }

  def castToFuzzyPredictionRecord(data: RDD[Vector], predict: RDD[Seq[(Int, Double)]], accessTime: Option[LocalDateTime] = None) = {

    val timeArray = accessTime match {
      case Some(arr) => arr
      case None => LocalDateTime.now()
    }

    data zip predict flatMap  (x => x._2.map( y => FuzzyPredictionRecord(timeArray.toString, x._1.toArray, y._1, y._2)))
  }

  def castToCrispPredictionRecord(data: RDD[Vector], predict: RDD[Int], accessTime: Option[LocalDateTime] = None) = {


    val timeArray = accessTime match {
      case Some(arr) => arr
      case None => LocalDateTime.now()
    }

    data zip predict map (x => CrispPredictionRecord(timeArray.toString, x._1.toArray, x._2))
  }

}
