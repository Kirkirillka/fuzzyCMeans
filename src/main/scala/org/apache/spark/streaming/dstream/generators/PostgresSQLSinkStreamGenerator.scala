package org.apache.spark.streaming.dstream.generators

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.streaming.dstream.generators.Utils.JDBCParams
import org.apache.spark.streaming.sources.StreamSource


/**
 * A Sink to save Streaming Batches to PostgresSQL
 *
 * @param params: parameters to connect to DB via JDBC
 * @param source: Underlying distribution source
 * @param session: Already provisioned Spark session to use
 * @param window: Size of sliding windows to iterate over in "run" section
 */
class PostgresSQLSinkStreamGenerator(val params: JDBCParams,
                                          override val source: StreamSource[Vector],
                                          override val session: SparkSession,
                                          val window: Int = 30)
  extends StreamGenerator[Vector](source, session) with Logging {


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

    val ds = this.source.sliding(window).foreach(r => {
      r.toDS().write
        .format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", s"jdbc:postgresql://${params("server")}/")
        .option("user", params("user"))
        .option("password", params("password"))
        .option("dbtable", s"${params("dbtable")}")
        // SaveMode.Append important for streaming batches, Spark checks if table exists.
        .mode(SaveMode.Append)
        .save()
    }

    )

  }
}