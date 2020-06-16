package org.apache.spark.streaming.adapters


object Models {

  case class TimestampVector(timestamp: String, vector: Array[Double])

}
