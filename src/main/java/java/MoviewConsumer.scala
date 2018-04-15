

import java.sql.Connection

import StreamingRecommender.{createUpdatedRatings, getSimilarMovies, getUserRecentRatings, updateRecommends2DB}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
//import kafka.serializer.Decoder
object MoviewConsumer {/*
  val sparkConf = new SparkConf().setAppName("MovieConsumer").setMaster("local[2]")
  val ssc = new StreamingContext(sparkConf, Seconds(10))
  var connection=ConnectPoolUtil.getConnection()






  def getUserRecentRatings(K: Int, userId: Int):Array[(Int, Double)]={
    //function feature: 通过MYSQL中评分的时间戳大小来获取userId的最近K-1次评分，与本次评分组成最近K次评分
    //return type：最近K次评分的数组，每一项是<movieId, rate>
    var connection: Connection = null

    var rate=0.0
    var movieId=0
    val arrayBuffer=mutable.ArrayBuffer[(Int, Double)]()
    try {

      connection=ConnectPoolUtil.getConnection()
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("select movieId,rate from user_rating_table where userId ="+userId+ " order by timestamp Desc " +
        "limit "+K)

      while (resultSet.next()) {
        rate = resultSet.getString("rate").toDouble
        movieId=resultSet.getString("movieId").toInt
        arrayBuffer+=((movieId,rate))
      }
    } catch {
      case e => e.printStackTrace
      //case _: Throwable => println("ERROR")
    }
    //connection.close()
    connection.close()
    arrayBuffer.toArray
    return Array(Tuple2(2,2.3))
  }







  def main(args: Array[String]): Unit = {
    var ratinginfo =new  ListBuffer[Tuple2[Int, Float]]
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    if(args.length!=4){
      println("缺少： <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum,groupId,topics,numThreads)=args

    val topicMap=topics.split(",").map((_,numThreads.toInt)).toMap
    val messages=KafkaUtils.createStream(ssc,zkQuorum,groupId,topicMap)

    val ratings=messages.map(_._2)
    ratings.count().print()
    val cleanData=ratings.map(
      line=> {
        val info = line.split(",")
        println(info.count(s=>true))
        var userid = info(0).toInt
        var movieid = info(1).toInt
         var rating = info(2).toFloat
        var time = info(3)
        UserRating(userid, movieid, rating, time)
        //println(userid, movieid, rating, time)
      }
    )
    cleanData.foreachRDD(
      rdd => {
        if (!rdd.isEmpty) {
          rdd.map{ case (userId, movieId, rate:Float, startTimeMillis) =>
            //获取近期评分记录
           // val recentRatings = getUserRecentRatings(5,userId = userId)
            var recentRatings=Array(Tuple2(userId,rate))
            //获取备选电影
            val candidateMovies = getSimilarMovies(Array(movieId))
            //为备选电影推测评分结果
           val updatedRecommends = createUpdatedRatings(recentRatings, candidateMovies)
            updateRecommends2DB(updatedRecommends)
          }.count()
        }
    })
//cleanData.print()
   // ratinginfo=ratinginfo.++(List(Tuple2(movieid,rating)))
//ratinginfo=ratinginfo.++(List(Tuple2(cleanData,cleanData(2))))

//ratinginfo=ratinginfo:::Tuple2(cleanData(0),cleanData(2))
//println(cleanData)
    println(455)
println(ratinginfo)






ssc.start()
    ssc.awaitTermination()
  }
  case class UserRating(userid:Int,movieid:Int,score:Float,date:String)
  */
}
