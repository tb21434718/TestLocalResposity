

import kafka.serializer.StringDecoder

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.hive._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

object SparkDirectStream {/*
  def main(args: Array[String]) {
    //Duration对象中封装了时间的一个对象，它的单位是ms.
    val conf = new SparkConf().setAppName("SparkDirectStream").setMaster("spark://master:7077")
    val batchDuration = new Duration(5000)
    val ssc = new StreamingContext(conf, batchDuration)
    val hc = new HiveContext(ssc.sparkContext)
    val validusers = hc.sql("select * from trainingData")
    val userlist = validusers.select("userId")

    val modelpath = "/tmp/bestmodel/0.8215454233270015"
    val broker = "master:9092"
    val topics = "test".split(",").toSet
    val kafkaParams = Map("bootstrap.servers" -> "master:9092")

    def exist(u: Int): Boolean = {
      val userlist = hc.sql("select distinct(userid) from trainingdata").rdd.map(x => x.getInt(0)).toArray()
      userlist.contains(u)
    }

    //为没有登录的用户推荐电影有不同的策略：
    //1.推荐观看人数较多的电影，这里采用这种策略
    //2.推荐最新的电影
    //    def recommendPopularMovies() = {
    //      val defaultrecresult = hc.sql("select * from top5DF").show
    //    }

    val defaultrecresult = hc.sql("select * from top5DF").rdd.toLocalIterator

    //创建SparkStreaming接收kafka消息队列数据的2种方式
    //一种是Direct approache,通过SparkStreaming自己主动去Kafka消息队
    //列中查询还没有接受进来的数据，并把他们拿到sparkstreaming中。pull
    val kafkaDirectStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    val messages = kafkaDirectStream.foreachRDD { rdd =>
      //      val userrdd = rdd.map(x => x._2.split("|")).map(x => x(1)).map(_.toInt)
      //      rdd.foreach{record =>
      //        val user = record._2.split("|").apply(1).toInt
      //        val model = MatrixFactorizationModel.load(sc,modelpath)
      //        val recresult = model.recommendProducts(user, 5)
      //        println(recresult)
      //      }
      val model = MatrixFactorizationModel.load(ssc.sparkContext, modelpath)
      val userrdd = rdd.map(x => x._2.split("|")).map(x => x(1)).map(_.toInt)
      val validusers = userrdd.filter(user => exist(user))
      val newusers = userrdd.filter(user => !exist(user))
      //可以采用迭代器的方式来避开对象不能序列化的问题。通过对RDD中的每个元素实时产生推荐结果，将结果写入到redis，或者其他高速缓存中，来达到一定的实时性。
      //2个流的处理分成2个sparkstreaming的应用来处理。
      val validusersIter = validusers.toLocalIterator
      val newusersIter = newusers.toLocalIterator
      while (validusersIter.hasNext) {
        val recresult = model.recommendProducts(validusersIter.next, 5)
        println("below movies are recommended for you :")
        println(recresult)
      }
      while (newusersIter.hasNext) {
        println("below movies are recommended for you :")
        for (i <- defaultrecresult) {
          println(i.getString(0))
        }
      }
      //      validusers.foreachPartition { partition =>
      //        while (partition.hasNext) {
      //          val recresult = model.recommendProducts(partition.next, 5)
      //          println("below movies are recommended for you :")
      //          println(recresult)
      //        }
      //      }
      //
      //      newusers.foreachPartition { partition =>
      //        while (partition.hasNext) {
      //          println("below movies are recommended for you :")
      //          for (i <- defaultrecresult) {
      //            println(i.getString(0))
      //          }
      //        }
      //      }
    }
    ssc.start()
    ssc.awaitTermination()
  }*/
}

