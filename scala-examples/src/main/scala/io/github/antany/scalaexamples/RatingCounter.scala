package io.github.antany.scalaexamples

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object RatingCounter {
 
  def main(args: Array[String]): Unit = {
    val inputFileLocation = "../data/ratings.dat";
    val conf = new SparkConf().setAppName("RatingCounter").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    val actData = sc.textFile(inputFileLocation, 5);
    val ratings = actData.map(split).countByValue().toSeq.sortBy(_._1);
    
    ratings.foreach(println);
    
  }
  
  def split(line: String): (Int) = {
    val lineArray = line.split("::");
    lineArray(2).toInt;
    
  }
}