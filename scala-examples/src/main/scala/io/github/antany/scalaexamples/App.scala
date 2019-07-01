package io.github.antany.scalaexamples

/**
 * @author ${user.name}
 */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object App {
  def main(args: Array[String]) {
    val logFile = args(0) // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application Test")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 5)
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
