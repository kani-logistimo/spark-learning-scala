package com.logistimo.learning

import java.sql.Timestamp

import com.datastax.spark.connector.SomeColumns
import org.apache.spark.{SparkContext, SparkConf}

import com.datastax.spark.connector._

/**
 * Created by mohansrinivas on 6/2/16.
 */
object LoadCassandra {

  case class dayslice_user(did:String,uid:String,date:String,sq:BigDecimal,tc:Long)
  def main(args: Array[String]): Unit = {

    val master = args(0)
    val cassandraHost = args(1)
    val cassandraKeyspace = args(2)
    val cassandraTable = args(3)
    val inputFile = args(4)

    val conf = new SparkConf().setAppName("CassandraLoader")
      .setMaster(master)
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.executor.memory","10g")
      .set("spark.cassandra.output.consistency.level","ANY")
      .set("spark.cassandra.output.concurrent.writes","20")
      .set("spark.eventLog.enabled", "true")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val data = sc.textFile(inputFile).map(_.split(","))
    val userData = data.map(p=>dayslice_user(p(0),p(1),p(2),p(3).toLong,p(4).toLong))
    userData.saveToCassandra(cassandraKeyspace,cassandraTable)

  }
}

