package com.logistimo.learning

import java.sql.Timestamp

import com.datastax.spark.connector.SomeColumns
import org.apache.spark.{SparkContext, SparkConf}

import com.datastax.spark.connector._

/**
 * Created by mohansrinivas on 6/2/16.
 */
object LoadCassandra {

  //case class Trend(domain_id: BigInt, t: Timestamp, mid: BigInt, kid: BigInt, cs: Double, q: Double, ty: String)

  def main(args: Array[String]): Unit = {
    if(args.length != 5){
      System.out.println("Not valid input parameters")
      System.out.println("LoadCassandra master_url cassandra_host inputdir")
    }

    val master = args(0)
    val cassandra_host = args(1)
    val cassandra_keyspace = args(2)
    val cassandra_table = args(3)
    val inputDir = args(4)

    val conf = new SparkConf().setAppName("CassandraLoader").setMaster(master).set("spark.cassandra.connection.host", cassandra_host).set("spark.eventLog.enabled", "true")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val data = sc.textFile("hdfs://cassandra1:9000/cassandra/dataextracted/part-0000*")
    //val testData = sc.wholeTextFiles(inputDir).map(_.split(","))
    //val data = testData.map(p => Trend(p(0).trim.toLong, Timestamp.valueOf((p(1))), p(2).trim.toLong, p(3).trim.toLong, p(4).trim.toDouble, p(5).trim.toDouble, p(6)))
    //data.saveToCassandra("spark_aggr", "material_trend", columns = SomeColumns("domain_id", "t", "mid", "kid", "cs", "q", "ty"))
  }
}

