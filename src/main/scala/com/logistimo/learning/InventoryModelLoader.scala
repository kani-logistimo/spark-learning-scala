package com.logistimo.learning

import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.spark.connector._

/**
 * Created by mohansrinivas on 5/31/16.
 */
object InventoryModelLoader {
  //case class dayslice_material(did:String,mid:String,date:String,tc:BigInt,sq:BigInt)
  def main (args : Array[String]): Unit = {
    if(args.length != 5){
      System.out.println("Invalid Input Parameters")
      System.out.println("Usage InventoryModelLoader masterip  cassandrahost cassandra_keyspace cassandra_table inputfile ")
      System.exit(1)
    }
    val master = args(0)
    val cassandraHost = args(1)
    val inputFile = args(4)
    val casssandraKeySpace = args(2)
    val cassandraTable = args(3)


    val conf = new SparkConf().setAppName("CassandraLoader").setMaster(master).set("spark.cassandra.connection.host", cassandraHost).set("spark.eventLog.enabled", "true")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(inputFile)

    val finalOutput = lines.map(
      line => map(line)).reduceByKey{
      case(x, y) => reduce(x, y)
    }

    //finalOutput.values.saveAsTextFile("/tmp/sparkjobfile1")
    //val data = finalOutput.values.map(p => dayslice_material(p.dids,p.mids,p.tss,p.stks.toLong,p.cnts.toLong))
    finalOutput.values.saveToCassandra(casssandraKeySpace,cassandraTable)
  }

  def map(line: String):(String, InventoryModel) ={
    val lineArray = line.split(",")
    (lineArray(0) + "-" + lineArray(3) + "-" + lineArray(4), new InventoryModel(lineArray(0), lineArray(3), lineArray(4), lineArray(6), 0))
  }

  def reduce(x: InventoryModel,y:InventoryModel):InventoryModel= {
    x.sq = (x.sq.toInt + y.sq.toInt).toString
    x.tc = (x.tc.toInt + 1)
    x
  }



}
