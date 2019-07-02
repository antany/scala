package io.github.antany.scalaexamples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source
import scala.io.Codec
import java.nio.charset.CodingErrorAction

object PopularMovieWithName {

  def main(args: Array[String]): Unit = {
    val inputFileLocation = "../data/ratings.dat";
    val conf = new SparkConf().setAppName("PopularrMovie").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val bcdata = sc.broadcast(movieNames);

    val data = sc.textFile(inputFileLocation, 4).map(x => (bcdata.value(movie(x).toInt), 1)).reduceByKey(_ + _).sortBy(_._2, false, 4).collect();

    data.foreach(println)

  }
  
  def movieNames : Map[Int,String] = {
    
    implicit val codec = Codec("UTF-8");
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE);
    
    val lines = Source.fromFile("../data/movies.dat").getLines();
    var movieData:Map[Int,String] = Map();
    for(line <- lines){
      var mov = line.split("::");
      if(mov.length >=2){
        movieData += (mov(0).toInt->mov(1));
      }
    }
    return movieData;
  }

  def movie(line: String): (String) = {
    var words = line.split("::");
    (words(1))
  }
}