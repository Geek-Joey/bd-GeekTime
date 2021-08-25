package rdd

import org.json4s.scalap.Main

/**
 * Distcp的spark实现
 *
 * sparkDistCp hdfs://xxx/source hdfs://xxx/target得到的结果为，启动多个task/executor，将hdfs://xxx/source目录复制到hdfs://xxx/target，得到hdfs://xxx/target/source
 * 需要支持source下存在多级子目录
 * 需支持-i Ignore failures 参数
 * 需支持-m max concurrence参数，控制同时copy的最大并发task数
 *
 * @author Joey
 * @create 2021-08-25 22:28
 */
object Distcp {
  def main(args: Array[String]): Unit = {

  }
}
