package org.apache.spark.streaming.adapters


object Models {

  case class TimestampVector(timestamp: String, vector: Array[Double])

  case class FuzzyPredictionRecord(timestamp: String,
                                   point: Array[Double],
                                   labels: Array[Int],
                                   membership: Array[Double]
                                 )

  case class CrispPredictionRecord(timestamp: String,
                                   point: Array[Double],
                                   label: Int
                                  )


}
