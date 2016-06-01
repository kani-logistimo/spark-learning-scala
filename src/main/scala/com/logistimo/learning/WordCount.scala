package com.logistimo.learning

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by kaniyarasu on 02/05/16.
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    // Create a Scala Spark Context.
    val conf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(conf)
    // Load our input data.
    val input = sc.textFile("/Users/kaniyarasu/Downloads/exampleJson.json")
    // Split it up into words.
    val words = input.flatMap(line => line.split(" "))
    // Transform into pairs and count.
    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
    // Save the word count back out to a text file, causing evaluation.
    counts.saveAsTextFile("/Users/kaniyarasu/sc_wc_out2")
  }
}
