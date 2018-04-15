

import org.apache.kafka.clients._
import org.apache.kafka.clients.producer._

import java.util.Properties

object KafkaProducer extends  {/*
  def main(args: Array[String]) {
    val testDF = hc.sql("select * from testData")
    val prop = new Properties()
    prop.put("bootstrap.servers", "master:9092")
    prop.put("acks", "all")
    prop.put("retries", "0")
    prop.put("batch.size", "16384")
    prop.put("linger.ms", "1")
    prop.put("buffer.memory", "33554432")
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val topic = "test"
    val testData = testDF.map(x => (topic, x.getInt(0).toString() + "|" + x.getInt(1).toString + "|" + x.getDouble(2).toString()))
    val producer = new KafkaProducer[String, String](prop)
    val messages = testData.toLocalIterator

    while (messages.hasNext) {
      val message = messages.next()
      val record = new ProducerRecord[String, String](topic, message._1, message._2)
      println(record)
      producer.send(record)
      Thread.sleep(1000)
    }
    //会有序列化的问题
    //    for (x <- testData) {
    //      val message = x
    //      val record = new ProducerRecord[String, String]("test", message._1, message._2)
    //      println(record)
    //      producer.send(record)
    //      Thread.sleep(1000)
    //    }

    //为什么不用testData.map或者foreach方法是因为这两种方法会让你的数据做分布式计算，在计算时，处理数据是无序的。
    //    testData.foreach
  }*/
}