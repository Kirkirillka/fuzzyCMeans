package org.apache.spark.streaming

import java.io.InputStreamReader

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object Utils {

  def getConfig(configName: Option[String] = None): Config = {
    val initialConf: Config = configName match {
      case _ => ConfigFactory.load()
    }

    val useHDFS = initialConf.getBoolean("apache.spark.use_config_on_hdfs")
    val conf: Config = if (useHDFS) {
      val hdfsAddress = initialConf.getString("apache.spark.hadoop.address")
      val path = configName match {
        case Some(a) => a
        case None => initialConf.getString("apache.spark.hadoop.conf_path")
      }

      val hdfsConf = new Configuration()

      hdfsConf.set("fs.defaultFS", hdfsAddress)

      val hdfs = FileSystem.get(hdfsConf)
      val stream = new InputStreamReader(hdfs.open(new Path(path)))

      ConfigFactory.parseReader(stream)
    } else {
      initialConf
    }

    conf
  }

}
