package com.logistimo.learning

import org.apache.hadoop.io.NullWritable
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

/**
 * Created by mohansrinivas on 5/31/16.
 */

class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  override def generateActualKey(key: Any, value: Any): Any =
    NullWritable.get()

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
    key.asInstanceOf[String].split("-")(0)
}

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
   // finalOutput.values.saveAsTextFile(outputDir)
    finalOutput.saveAsHadoopFile(outputDir,classOf[String], classOf[String],
      classOf[RDDMultipleTextOutputFormat])
  }

  def map(line: String):(String, InventoryModel) ={
    val lineArray = line.split(",")
    ("USER"+"-"+lineArray(0) + "-" + lineArray(1) + "-" + lineArray(4), new InventoryModel(lineArray(0), lineArray(1), lineArray(4), lineArray(6), 1))
    ("KIOSK"+"-"+lineArray(0) + "-" + lineArray(2) + "-" + lineArray(4), new InventoryModel(lineArray(0), lineArray(2), lineArray(4), lineArray(6), 1))
    ("MATERIAL"+"-"+lineArray(0) + "-" + lineArray(3) + "-" + lineArray(4), new InventoryModel(lineArray(0), lineArray(3), lineArray(4), lineArray(6), 1))
  }

  def reduce(x: InventoryModel,y:InventoryModel):InventoryModel= {
    x.sq = (x.sq.toInt + y.sq.toInt).toString
    x.tc = (x.tc.toInt + 1)
    x
  }


}