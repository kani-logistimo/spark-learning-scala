package com.logistimo.learning

import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.spark.connector._

/**
 * Created by mohansrinivas on 5/31/16.
 */
object InventoryModelLoader {
  def main (args : Array[String]): Unit = {
    if(args.length != 3){
      System.out.println("Invalid Input Parameters")
      System.out.println("Usage InventoryModelLoader masterURL   inputFile outputDir ")
      System.exit(1)
    }
    val master = args(0)
    val inputFile = args(1)
    val outputDir = args(2)


    val conf = new SparkConf().setAppName("HDFS Loader")
      .setMaster(master).set("spark.eventLog.enabled", "true")
      .set("spark.executor.memory","10g")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(inputFile)
    val finalOutput = lines.map(
      line => map(line)).reduceByKey{
      case(x, y) => reduce(x, y)
    }
    finalOutput.values.saveAsTextFile(outputDir)
  }

  def map(line: String):(String, InventoryModel) ={
    val lineArray = line.split(",")
    (lineArray(0) + "-" + lineArray(1) + "-" + lineArray(4), new InventoryModel(lineArray(0), lineArray(1), lineArray(4), lineArray(6), 1))
    (lineArray(0) + "-" + lineArray(2) + "-" + lineArray(4), new InventoryModel(lineArray(0), lineArray(2), lineArray(4), lineArray(6), 1))
    (lineArray(0) + "-" + lineArray(3) + "-" + lineArray(4), new InventoryModel(lineArray(3), lineArray(2), lineArray(4), lineArray(6), 1))
  }

  def reduce(x: InventoryModel,y:InventoryModel):InventoryModel= {
    x.sq = (x.sq.toInt + y.sq.toInt).toString
    x.tc = (x.tc.toInt + 1)
    x
  }


}