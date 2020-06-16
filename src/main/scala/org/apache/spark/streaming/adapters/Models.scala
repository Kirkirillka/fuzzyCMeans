package org.apache.spark.streaming.adapters


object Models {

  case class ClusterRecord(timestamp: String, vector: Array[Double])

}
