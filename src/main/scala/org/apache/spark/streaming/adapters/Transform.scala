package org.apache.spark.streaming.adapters

import java.time.LocalDateTime

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.adapters.Models.{CrispPredictionRecord, FuzzyPredictionRecord, TimestampVector}

object Transform {

  def castToTimestampVector(data: RDD[Vector], accessTime: Option[RDD[LocalDateTime]] = None): RDD[TimestampVector] = {

    val timeArray = accessTime match {
      case Some(arr) => arr
      case None => data.map(x => LocalDateTime.now())
    }

    data zip timeArray  map(x => TimestampVector(x._2.toString, x._1.toArray))

  }

  def castToFuzzyPredictionRecord(data: RDD[Vector], predict: RDD[Seq[(Int,Double)]], accessTime: Option[RDD[LocalDateTime]] = None) = {

    val timeArray = accessTime match {
      case Some(arr) => arr
      case None => data.map(x => LocalDateTime.now())
    }

    data zip predict zip timeArray map(x => FuzzyPredictionRecord(x._2.toString,x._1._1.toArray,x._1._2.map(_._1).toArray,x._1._2.map(_._2).toArray))
  }

  def castToCrispPredictionRecord(data: RDD[Vector], predict: RDD[Int],  accessTime: Option[RDD[LocalDateTime]] = None) = {

    val timeArray = accessTime match {
      case Some(arr) => arr
      case None => data.map(x => LocalDateTime.now())
    }


    data zip predict zip timeArray map(x => CrispPredictionRecord(x._2.toString, x._1._1.toArray, x._1._2))
  }

}
