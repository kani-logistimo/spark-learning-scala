package com.logistimo.learning

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by mohansrinivas on 6/2/16.
 */
object DataSet_Analysis {

  def main (args : Array[String]): Unit = {
    val conf = new SparkConf().setAppName("My App").setMaster("local").set("spark.cassandra.connection.host", "localhost").set("spark.eventLog.enabled", "true")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)


    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost/logistimo"
    val prop = new java.util.Properties
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")
    prop.setProperty("driver", driver)
    val t = sqlContext.read.jdbc(url, "transaction", prop).select("KEY", "KID", "MID", "TYPE", "Q", "CS", "T")
    val td = sqlContext.read.jdbc(url, "transaction_domains", prop).select("KEY_OID", "DOMAIN_ID")
    val joined = t.join(td, t.col("KEY") <=> td.col("KEY_OID")).select("DOMAIN_ID", "T", "MID", "KID", "CS", "Q", "TYPE")
    val newNames = Seq("domain_id", "t", "mid", "kid", "cs", "q", "ty")
    //joined.toDF(newNames: _*).printSchema()

    joined.rdd.map(_.toSeq.map(_.toString).mkString(",")).saveAsTextFile("hdfs://localhost:9000/user/spark/spark_001")
  }

}
