package io.github.antany.scalaexamples

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object AverageUserAgeVsRateCount {
  def main(args: Array[String]): Unit = {
    val inputFileLocation = "../data/users.dat";
    val conf = new SparkConf().setAppName("RatingCounter").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    val data = sc.textFile(inputFileLocation,4);
    val workData = data.map(split);
    
    val totalByAge = workData.mapValues(x=>(x.toInt,1)).reduceByKey((x,y)=>(x._1+y._1, x._2 + y._2))
    
    totalByAge.collect().foreach(println);
    
    totalByAge.mapValues(x=>x._1.toFloat/x._2.toFloat).sortByKey(true, 4).collect().foreach(println);
    
  }
  
   def split(line: String): (String, String) = {
    val lineArray = line.split("::");
    (lineArray(2),lineArray(3));
    
  }
}