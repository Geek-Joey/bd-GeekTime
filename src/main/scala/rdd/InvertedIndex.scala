package rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
 * 使用RDD API实现带词频的倒排索引
 *
 * @author Joey
 * @create 2021-08-25 21:38
 */
object InvertedIndex {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("InvertedIndex")
    val sc: SparkContext = new SparkContext(conf)
    val fileSource: Array[String] = Source.fromFile("src/main/data/index.txt").getLines().toArray
    val fileRDD: RDD[String] = sc.parallelize(fileSource)

    //扁平化操作
    val flatMapRDD: RDD[(String, String)] = fileRDD.flatMap{
      lines => {
        val line: Array[String] = lines.split(".")
        line(1).split(",").map {
          value => (value, line(0))
        }
      }
    }

    val groupRDD: RDD[(String, Iterable[String])] = flatMapRDD.groupByKey()
    val sortRDD: RDD[(String, Iterable[String])] = groupRDD.sortBy(_._1)
    sortRDD.foreach {
      word => println(s"${word._1}|${word._2.mkString(",")}")
    }
  }
}
