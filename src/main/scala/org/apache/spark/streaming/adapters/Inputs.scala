package org.apache.spark.streaming.adapters

import org.apache.spark.streaming.dstream.generators.Utils.{JDBCParams, KafkaParams}

object Inputs {


  def _fromPostgresSQL(params: JDBCParams,
              window: Int = 30)  = {

  }

  def _fromKafka(params: KafkaParams,
                        window: Int = 30)  = {

  }


  def _fromSocket(params: KafkaParams,
                  window: Int = 30)  = {

  }


}
