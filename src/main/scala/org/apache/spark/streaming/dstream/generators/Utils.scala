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

  val ClusterSaverJDBCParams: JDBCParams = {

    val defaultConf = DefaultJDBCParams

    defaultConf.updated("dbtable","clusters")

  }

  val DataPointsSaverJDBCParams: JDBCParams = {

    val defaultConf = DefaultJDBCParams

    defaultConf.updated("dbtable","points")

  }

  object SFCMDefaultClusterAlgorithmsParams {

    val SFCMClusterSaverJDBCParams: JDBCParams = {

      val defaultConf = DefaultJDBCParams

      defaultConf.updated("dbtable","SFCM_clusters")

    }

    val SFCMDataPointsSaverJDBCParams: JDBCParams = {

      val defaultConf = DefaultJDBCParams

      defaultConf.updated("dbtable","SFCM_points")

    }

  }

  object UMicroDefaultClusterAlgorithmsParams {

    val UMicroClusterSaverJDBCParams: JDBCParams = {

      val defaultConf = DefaultJDBCParams

      defaultConf.updated("dbtable","UMicro_clusters")

    }

    val UMicroDataPointsSaverJDBCParams: JDBCParams = {

      val defaultConf = DefaultJDBCParams

      defaultConf.updated("dbtable","UMicro_points")

    }

  }


  object dFuzzyStreamDefaultClusterAlgorithmsParams {

    val dFuzzyStreamClusterSaverJDBCParams: JDBCParams = {

      val defaultConf = DefaultJDBCParams

      defaultConf.updated("dbtable","dFuzzyStream_clusters")

    }

    val dFuzzyStreamDataPointsSaverJDBCParams: JDBCParams = {

      val defaultConf = DefaultJDBCParams

      defaultConf.updated("dbtable","dFuzzyStream_points")

    }

  }
}
