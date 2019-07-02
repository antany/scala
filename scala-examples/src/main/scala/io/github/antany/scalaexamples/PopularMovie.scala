package io.github.antany.scalaexamples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object PopularMovie {

  def main(args: Array[String]): Unit = {
    val inputFileLocation = "../data/ratings.dat";
    val conf = new SparkConf().setAppName("PopularrMovie").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val data = sc.textFile(inputFileLocation, 4).map(x => (movie(x), 1)).reduceByKey(_ + _).sortBy(_._2, false, 4).collect();

    data.foreach(println)

  }

  def movie(line: String): (String) = {
    var words = line.split("::");
    (words(1))
  }
}