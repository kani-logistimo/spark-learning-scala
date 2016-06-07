package com.logistimo.learning

import com.datastax.spark.connector._
import com.datastax.spark.connector.writer.{WriteConf, TTLOption}
import com.logistimo.learning.InventoryTrendModel
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by kaniyarasu on 03/05/16.
 */


/*class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  override def generateActualKey(key: Any, value: Any): Any =
    NullWritable.get()

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
    key.asInstanceOf[String]
}*/

object InventoryTrends {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local").setAppName("InventoryTrends").set("spark.cassandra.connection.host", "127.0.0.1")
    val sparkContext = new SparkContext(sparkConf)

    val transactionInput = sparkContext.textFile("hdfs://localhost:9000/transaction/transactions_dump.csv")
      /*.filter(line => line.contains("1343802"))*/

    //println("1343802 had " + transactionInput.count() + " concerning transactions, starting process!!")
    val finalOutput = transactionInput.map(line => map(line))
      .sortByKey()
      .reduceByKey{case(x, y) => reduce(x, y)}

    finalOutput.saveAsHadoopFile("hdfs://localhost:9000/transaction/out" + System.currentTimeMillis(), classOf[String], classOf[String],
      classOf[RDDMultipleTextOutputFormat])

    finalOutput.values.saveToCassandra("test", "inventorytrendsbymaterial", SomeColumns("did", "mid", "i_date", "count"), writeConf = WriteConf(ttl = TTLOption.constant(100)))
  }

  def map(line: String): (String, InventoryTrendModel) ={
    val lineArray = line.split("\t")
//    (lineArray(21) + "-" + lineArray(11) + "-" + lineArray(23).split(" ")(0), new InventoryTrendModel(lineArray(21), lineArray(11), lineArray(23).split(" ")(0), lineArray(19)))
    (lineArray(21), new InventoryTrendModel(lineArray(21), lineArray(11), lineArray(23).split(" ")(0), lineArray(19)))
  }

  def reduce(x: InventoryTrendModel, y: InventoryTrendModel): (InventoryTrendModel) ={
    x.count = (x.count.toDouble + y.count.toDouble).toString
    x
  }
}
