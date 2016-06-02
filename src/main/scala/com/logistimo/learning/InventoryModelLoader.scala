package com.logistimo.learning

import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.spark.connector._

/**
 * Created by mohansrinivas on 5/31/16.
 */
object InventoryModelLoader {
  def main (args : Array[String]): Unit = {
    if(args.length != 5){
      System.out.println("Invalid Input Parameters")
      System.out.println("Usage InventoryModelLoader masterip  cassandrahost cassandr_-keyspace cassandra_table inputfile ")
      System.exit(1)
    }
    val master = args(0)
    val cassandraHost = args(1)
    val inputFile = args(4)
    val casssandraKeySpace = args(2)
    val cassandraTable = args(3);

    val conf = new SparkConf().setAppName("CassandraLoader").setMaster(master).set("spark.cassandra.connection.host", cassandraHost).set("spark.eventLog.enabled", "true")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(inputFile)


    val finalOutput = lines.map(
      line => map(line)).reduceByKey{
      case(x, y) => reduce(x, y)
    }
    //finalOutput.values.saveToCassandra(casssandraKeySpace,cassandraTable)
    finalOutput.values.saveAsTextFile("hdfs://cassandra1:9000/cassandra/dataextracted")
  }

  def map(line: String):(String, InventoryModel) ={
    val lineArray = line.split(",")
    /*ty match {
      case "M" => {*/
        (lineArray(0) + "-" + lineArray(1) + "-" + lineArray(4), new InventoryModel(lineArray(0), lineArray(1), lineArray(4), lineArray(6), 1))
     /* }
      case "K" => { (lineArray(0) + "-" + lineArray(2) + "-" + lineArray(4), new InventoryModel(lineArray(0), lineArray(2), lineArray(4), lineArray(6), 0))}
      case "U" => { (lineArray(0) + "-" + lineArray(1) + "-" + lineArray(4), new InventoryModel(lineArray(0), lineArray(1), lineArray(4), lineArray(6), 0))}*/

   // }
  }

  def reduce(x: InventoryModel,y:InventoryModel):InventoryModel= {
    x.sq = (x.sq.toInt + y.sq.toInt).toString
    x.tc = (x.tc.toInt + 1)
    x
  }


}