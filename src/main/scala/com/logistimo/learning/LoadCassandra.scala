package com.logistimo.learning

import java.sql.Timestamp

import com.datastax.spark.connector.SomeColumns
import org.apache.spark.{SparkContext, SparkConf}

import com.datastax.spark.connector._

/**
 * Created by mohansrinivas on 6/2/16.
 */
object LoadCassandra {

  case class Trend(domain_id: BigInt, t: Timestamp, mid: BigInt, kid: BigInt, cs: Double, q: Double, ty: String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("My App").setMaster("local").set("spark.cassandra.connection.host", "localhost").set("spark.eventLog.enabled", "true")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val testData = sc.textFile("hdfs://localhost:9000/user/spark/spark_001").map(_.split(","))
    val data = testData.map(p => Trend(p(0).trim.toLong, Timestamp.valueOf((p(1))), p(2).trim.toLong, p(3).trim.toLong, p(4).trim.toDouble, p(5).trim.toDouble, p(6)))
    data.saveToCassandra("spark_aggr", "material_trend", columns = SomeColumns("domain_id", "t", "mid", "kid", "cs", "q", "ty"))
  }
}

