package com.logistimo.learning

import java.sql.Timestamp

import com.datastax.spark.connector.SomeColumns
import org.apache.spark.{SparkContext, SparkConf}

import com.datastax.spark.connector._

/**
 * Created by mohansrinivas on 6/2/16.
 */
object LoadCassandra {

  case class dayslice_user(did:String,id:String,date:String,sq:BigDecimal,tc:Long)
  def main(args: Array[String]): Unit = {
    val master = "spark://52.207.246.150:7077"
    val cassandra_host = "52.207.246.150"

    val conf = new SparkConf().setAppName("CassandraLoader").setMaster(master).set("spark.cassandra.connection.host", cassandra_host).set("spark.eventLog.enabled", "true")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val data = sc.textFile("hdfs://cassandra1:9000/cassandra/dataextracted/part-0000*").map(_.split(","))
    //val testData = sc.wholeTextFiles(inputDir).map(_.split(","))
    val dataa = data.map(p=>dayslice_user(p(0),p(1),p(2),p(3).toLong,p(4).toLong))
    dataa.saveToCassandra("testdb","dayslice_user")

    //val data = testData.map(p => Trend(p(0).trim.toLong, Timestamp.valueOf((p(1))), p(2).trim.toLong, p(3).trim.toLong, p(4).trim.toDouble, p(5).trim.toDouble, p(6)))
    //data.saveToCassandra("spark_aggr", "material_trend", columns = SomeColumns("domain_id", "t", "mid", "kid", "cs", "q", "ty"))
  }
}

