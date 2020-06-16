package org.apache.spark.streaming.adapters

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.adapters.Models.TimestampVector

object Transform {

  def castToTimestampVector(data: RDD[Vector]): RDD[TimestampVector] = {

    // Append time of new entries added
    val currentTime = java.time.LocalDateTime.now().toString

    data.map(x => TimestampVector(currentTime,x.toArray))

  }

}
