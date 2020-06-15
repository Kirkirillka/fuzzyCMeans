package org.apache.spark.streaming.dstream.processors

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.generators.Utils.KafkaParams
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

class GaussianKafkaStreamProcessor(val params: KafkaParams,
                                        val session: SparkSession) {

  val topics = params("servers").split(",")

  private val derivedKafkaParams = Map[String, Object](
    "bootstrap.servers" -> params("servers"),
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )


  def handle(obj: Seq[Vector[Double]]) = {
    println("Received" + obj)
  }


  def run() = {

    val streamingContext = new StreamingContext(session.sparkContext, Seconds(1))

    val derivedKafkaParams = Map[String, Object](
      "bootstrap.servers" -> params("servers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, derivedKafkaParams)
    )

    stream.map(record => record.value).foreachRDD(r =>
    {
      println(r)
      r
    }
    )

    streamingContext.start()
    streamingContext.awaitTermination()

  }


}