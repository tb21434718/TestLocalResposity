

import kafka.serializer.StringDecoder

import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.storage._

object SparkReceiverStream extends  {
  /*
  def main(args: Array[String]) {
    val batchDuration = new Duration(5)
    val ssc = new StreamingContext(sc, batchDuration)
    val validusers = hc.sql("select * from trainingData")

    val modelpath = "/tmp/bestmodel/0.8215454233270015"
    val broker = "master:9092"
    val topics = Map("test" -> 1)
    val kafkaParams = Map("broker" -> "master:9092")
    val zkQuorum = "localhost:9092"
    val groupId = 1
    val storageLevel = StorageLevel.MEMORY_ONLY
    //创建SparkStreaming接收kafka消息队列数据的第2种方式
    //Receiver base approach,这种方式是把sparkstreaming当作一个consumer来消费kafka中的消息，
    //可以通过启用WAL的方式来把这个stream做成强一致性.push的方式
    val kafkaStream = KafkaUtils.createStream(ssc, zkQuorum, "1", topics, storageLevel)
  }*/
}