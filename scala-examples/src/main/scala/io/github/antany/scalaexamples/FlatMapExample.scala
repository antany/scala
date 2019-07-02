package io.github.antany.scalaexamples

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object FlatMapExample {
 
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RatingCounter").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val inputString = sc.parallelize("ab bc 3 4 5 6");
    
    inputString.collect().foreach(println);
    
    //val flatData = inputString.flatMap(split);
    
  //  ratings.foreach(println);
    
  }
  
  def split(line: String): (Seq[String]) = {
    val lineArray = line.split(" ");
    lineArray.toSeq;
    
  }
}