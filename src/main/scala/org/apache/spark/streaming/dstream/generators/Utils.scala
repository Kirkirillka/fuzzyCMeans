package org.apache.spark.streaming.dstream.generators

import com.typesafe.config.{Config, ConfigFactory}

object Utils {

  type KafkaParams = Map[String, String]

  type JDBCParams = Map[String, String]


  val DefaultKafkaParams: KafkaParams = {
    val conf: Config = ConfigFactory.load()

    Map(
      "inTopics" -> conf.getString("apache.spark.kafka.inTopics"),
      "outTopics" -> conf.getString("apache.spark.kafka.outTopics"),
      "servers" -> conf.getString("apache.spark.kafka.bootstrap_servers")
    )


  }

  val DefaultJDBCParams: JDBCParams = {

    val conf: Config = ConfigFactory.load()

    Map(
      "server" -> conf.getString("apache.spark.jdbc.server"),
      "dbtable" -> conf.getString("apache.spark.jdbc.dbtable"),
      "user" -> conf.getString("apache.spark.jdbc.user"),
      "password" -> conf.getString("apache.spark.jdbc.password")
    )
  }

  val FCMClusterSaverJDBCParams: JDBCParams = {

    val defaultConf = DefaultJDBCParams

    defaultConf.updated("dbtable", "FCM_clusters")

  }


  val FCMDataPointsSaverJDBCParams: JDBCParams = {

    val defaultConf = DefaultJDBCParams

    defaultConf.updated("dbtable", "FCM_points")

  }

  val FCMFuzzyPredictSaverJDBCParams: JDBCParams = {

    val defaultConf = DefaultJDBCParams

    defaultConf.updated("dbtable", "FCM_predict")

  }

  object SFCMDefaultClusterAlgorithmsParams {

    val SFCMClusterSaverJDBCParams: JDBCParams = {

      val defaultConf = DefaultJDBCParams

      defaultConf.updated("dbtable", "SFCM_clusters")

    }

    val SFCMDataPointsSaverJDBCParams: JDBCParams = {

      val defaultConf = DefaultJDBCParams

      defaultConf.updated("dbtable", "SFCM_points")

    }

    val SFCMFuzzyPredictSaverJDBCParams: JDBCParams = {

      val defaultConf = DefaultJDBCParams

      defaultConf.updated("dbtable", "SFCM_predict")

    }

  }

  object UMicroDefaultClusterAlgorithmsParams {

    val UMicroClusterSaverJDBCParams: JDBCParams = {

      val defaultConf = DefaultJDBCParams

      defaultConf.updated("dbtable", "UMicro_clusters")

    }

    val UMicroDataPointsSaverJDBCParams: JDBCParams = {

      val defaultConf = DefaultJDBCParams

      defaultConf.updated("dbtable", "UMicro_points")

    }

    val UMicroFuzzyPredictSaverJDBCParams: JDBCParams = {

      val defaultConf = DefaultJDBCParams

      defaultConf.updated("dbtable", "UMicro_predict")

    }

  }


  object dFuzzyStreamDefaultClusterAlgorithmsParams {

    val dFuzzyStreamClusterSaverJDBCParams: JDBCParams = {

      val defaultConf = DefaultJDBCParams

      defaultConf.updated("dbtable", "dFuzzyStream_clusters")

    }

    val dFuzzyStreamDataPointsSaverJDBCParams: JDBCParams = {

      val defaultConf = DefaultJDBCParams

      defaultConf.updated("dbtable", "dFuzzyStream_points")

    }

    val dFuzzyStreamFuzzyPredictSaverJDBCParams: JDBCParams = {

      val defaultConf = DefaultJDBCParams

      defaultConf.updated("dbtable", "dFuzzyStream_predict")

    }

  }



  object UncertainStreamDefaultClusterAlgorithmsParams {

    val UncertainStreamClusterSaverJDBCParams: JDBCParams = {

      val defaultConf = DefaultJDBCParams

      defaultConf.updated("dbtable", "UncertainStream_clusters")

    }

    val UncertainStreamDataPointsSaverJDBCParams: JDBCParams = {

      val defaultConf = DefaultJDBCParams

      defaultConf.updated("dbtable", "UncertainStream_points")

    }

    val UncertainStreamFuzzyPredictSaverJDBCParams: JDBCParams = {

      val defaultConf = DefaultJDBCParams

      defaultConf.updated("dbtable", "UncertainStream_predict")

    }

  }



  object GroundTruthStreamDefaultClusterAlgorithmsParams {

    val GroundTruthStreamClusterSaverJDBCParams: JDBCParams = {

      val defaultConf = DefaultJDBCParams

      defaultConf.updated("dbtable", "GroundTruthStream_clusters")

    }

    val GroundTruthStreamDataPointsSaverJDBCParams: JDBCParams = {

      val defaultConf = DefaultJDBCParams

      defaultConf.updated("dbtable", "GroundTruthStream_points")

    }

    val GroundTruthStreamFuzzyPredictSaverJDBCParams: JDBCParams = {

      val defaultConf = DefaultJDBCParams

      defaultConf.updated("dbtable", "GroundTruthStream_predict")

    }

  }

}
