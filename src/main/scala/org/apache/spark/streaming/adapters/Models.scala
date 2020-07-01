package org.apache.spark.streaming.adapters


object Models {

  case class DataPointRecord(timestamp: String, centroid: Array[Double], unique_label: Long)

  case class FuzzyPredictionRecord(timestamp: String,
                                   data_point: Array[Double],
                                   cluster_label: Int,
                                   data_point_membership: Double
                                 )

  case class CrispPredictionRecord(timestamp: String,
                                   data_point: Array[Double],
                                   cluster_label: Int,
                                   data_point_membership: Double = 1.0
                                  )


}
