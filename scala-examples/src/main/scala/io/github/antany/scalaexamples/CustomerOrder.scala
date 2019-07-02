package io.github.antany.scalaexamples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CustomerOrder {
  
  def main(args: Array[String]): Unit = {
     val inputFileLocation = "../data/customer-orders.csv";
      val conf = new SparkConf().setAppName("RatingCounter").setMaster("local[*]")
      val sc = new SparkContext(conf)
      
      val actData = sc.textFile(inputFileLocation, 4).map(split).reduceByKey(_ + _).sortBy(_._2, false,4)
      actData.collect().foreach(println);
  }
  
  def split(line:String) : (Int,Float) ={
    var words = line.split(",");
    (words(0).toInt,words(2).toFloat)
  }
}