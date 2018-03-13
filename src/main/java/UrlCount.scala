
import java.util.HashMap

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}



object UrlCount{
  def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    return Some(newValues.sum+runningCount.getOrElse(0))
  }

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val Array(zkQuorm, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint("d://ck2")
    //"alog-2016-04-16,alog-2016-04-17,alog-2016-04-18"
    //"Array((alog-2016-04-16, 2), (alog-2016-04-17, 2), (alog-2016-04-18, 2))"
    val topicMap = "test2".split(",").map((_, 2)).toMap
    val data = KafkaUtils.createStream(ssc,"192.168.133.130:2181", group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)

  /*  data.foreachRDD{rdd=>
      val userrdd = rdd.map(x => x._2.split("\\")).map(x => x(1)).map(_.toInt)
      println("usreid"+userrdd)
      println("key"+rdd.map(x=>x._1))
     // rdd.foreach { parition =>

    }*/
    println("data")
    var lines=data.map(_._2)
    val rr=data.flatMap(_._2.split("|"))
    val t=rr.map(x=>x)
println(t)


    data.count().print()
  /*  println(1)
    //val words = data.map(_._2).flatMap(_.split(" "))
    //val wordCounts = words.map((_, 1)).updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
    //println(wordCounts.toString)

    val lines = data.map(_._2)flatMap(_.split(" "))
    val word = lines.map(x=>(x,1))
    word.count().print()
    word.print()
    //wordCounts.print()
*/
    ssc.start()
    println(222222)
    // word.print()


    //wordCounts.print()
    ssc.awaitTermination()
    println(3)
  }
}
