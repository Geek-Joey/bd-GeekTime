package rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //1.创建 sparkConf 配置信息对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //2.创建 sparkContext 上下文对象
    val sc: SparkContext = new SparkContext(sparkConf)

    //3.读取文件
    val fileRDD: RDD[String] = sc.textFile("E:\\ShowCode\\IdeaProjects\\bd-GeekTime\\src\\main\\data\\input.txt")

    //4.将文件中的数据进行分词
    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))

    //5.转换结构： word => (word,1)
    val word2TupleRDD: RDD[(String, Int)] = wordRDD.map((_, 1))

    //6.按照相同单词进行分组聚合
    val word2Count: RDD[(String, Int)] = word2TupleRDD.reduceByKey(_ + _)

    //7.将聚合结果采集到内存中
    val resultArray: Array[(String, Int)] = word2Count.collect()

    //8.打印结果
    resultArray.foreach(println)

    //9.关闭spark 连接
    sc.stop()
  }
}
