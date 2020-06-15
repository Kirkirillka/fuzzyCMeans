package org.apache.spark.streaming.dstream.generators

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.generators.Utils.KafkaParams
import org.apache.spark.streaming.sources.StreamSource
import org.apache.spark.mllib.linalg.{Vectors, Vector}


/**
 *
 * @param params: Parameters to connect to Kafka clusters
 * @param source: Underlying distribution source
 * @param session: Already provisioned Spark session to use
 * @param window: Size of sliding windows to iterate over in "run" section
 */
class KafkaSinkStreamGenerator(
                                    val params: KafkaParams,
                                    override val source: StreamSource[Vector],
                                    override val session: SparkSession,
                                    val window: Int = 30)
  extends StreamGenerator[Vector](source, session)
    with Logging {


  /**
   * Blocking function that start streaming
   */
  override def run(): Unit = {

    val ss = session
    import ss.implicits._

    // Not so intellectual, but works anyway.
    // Wanted to use Structured Streaming Kafka Sink, but there is problem with continuous Stream integration.
    // https://spark.apache.org/docs/2.2.0/structured-streaming-kafka-integration.html
    // Well, take each window-sized sequence of T batches, then save each of them to Kafka sink

    val ds = this.source.sliding(window).foreach(rdd => {

      logInfo(s"Reached ${rdd.length} new records. Writing to Kafka.")

      session.sparkContext.parallelize(rdd).toDF()
        .write
        .format("kafka")
        .option("topic", params("inTopics"))
        .option("kafka.bootstrap.servers", params("servers"))
        .save()

      logInfo(s"Sleep for ${timeout}")
      Thread.sleep(timeout)
    })}

}