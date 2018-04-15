import java.sql.Connection


import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object StreamingRecommender {

  //设置一些初始化参数
  val sparkConf = new SparkConf().setAppName("StreamingRecommender").setMaster("local[2]")
  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Seconds(10))
  val minSimilarity=0.5



  /*def getUserRecentRatings(collection: MongoCollection, K: Int, userId: Int, movieId: Int, rate: Double, timestamp: Long): Array[(Int, Double)] = {
    //function feature: 通过MONGODB中评分的时间戳大小来获取userId的最近K-1次评分，与本次评分组成最近K次评分
    //return type：最近K次评分的数组，每一项是<movieId, rate>
    val query = MongoDBObject("userId" -> userId)
    val orderBy = MongoDBObject("timestamp" -> -1)
    val recentRating = collection.find(query).sort(orderBy).limit(K - 1).toArray.map{ item =>
      (item.get("movieId").toString.toInt, item.get("rate").toString.toDouble)
    }.toBuffer
    recentRating += ((movieId, rate))
    //将本次评分写回到MONGODB，为了测试方便暂时不需要，上线再取消注释
    //collection.insert(MongoDBObject("userId" -> userId, "movieId" -> movieId, "rate" -> rate, "timestamp" -> timestamp))
    recentRating.toArray
  }*/

  //在数据库中读取用户的近期评分
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
  }


  //从数据库中读取数据,读取两个电影的相似度
  def getSimilarityBetween2Movies(movie1:Int,movie2:Int):Double= {
    var connection: Connection = null

    var rate=0.6
    try {

      connection=ConnectPoolUtil.getConnection()
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("select rate from l_similar where movie1 ="+movie1+ " and movie2="+movie2)

      while (resultSet.next()) {
      rate = resultSet.getString("rate").toDouble

      }
    } catch {
      case e => e.printStackTrace
      //case _: Throwable => println("ERROR")
    }
    //connection.close()
    connection.close()
    rate

}

  //在数据库中的相似度表中，按照电影ID查询相似的电影
  def getSimilarMovies(movieIds: Array[Int]):mutable.ArrayBuffer[Int]= {
    var connection: Connection = null

    var movies = mutable.ArrayBuffer[Int]()
    try {


      connection = ConnectPoolUtil.getConnection()
      for (movieId <- movieIds) {
        val statement = connection.createStatement()
        val resultSet = statement.executeQuery("select movie2 from l_similar where movie1 =" + movieId)

        while (resultSet.next()) {
          movies += resultSet.getString("movie2").toInt

        }
      }
    }
    catch
    {
      case e => e.printStackTrace
      //case _: Throwable => println("ERROR")
    }
    connection.close()
    movies
  }


  //计算候选项的优先级
  def log(m: Double): Double = math.log(m) / math.log(2)

  def createUpdatedRatings(recentRatings: Array[(Int, Double)],candidateMovies: mutable.ArrayBuffer[Int]): Array[(Int, Double)] ={
    //function feature: 核心算法，计算每个备选电影的预期评分
    //return type：备选电影预计评分的数组，每一项是<movieId, maybe_rate>
    val allSimilars = mutable.ArrayBuffer[(Int, Double)]()
    val increaseCounter = mutable.Map[Int, Int]()
    val reduceCounter = mutable.Map[Int, Int]()
    var sim_sum=0
    for(cmovieId<-candidateMovies;(rmovieId,rate)<-recentRatings){
        val sim=getSimilarityBetween2Movies(cmovieId,rmovieId)
        if(sim>minSimilarity) {
          allSimilars += ((cmovieId, sim * rate))
          sim_sum+=1
          //[(movieId,sim * rate)]
          if(rate>=3.0){
          increaseCounter(cmovieId)=increaseCounter.getOrElse(cmovieId,0)+1
        }else{
        reduceCounter(cmovieId)=reduceCounter.getOrElse(cmovieId,0)+1
      }
      }
      }
    allSimilars.toArray.groupBy{case (movieId,rate)=>movieId}
        .map{case (movieId,simArray)=>
          (movieId,simArray.map(_._2).sum/sim_sum
            +log(increaseCounter.getOrElse[Int](movieId,1))-log(reduceCounter.getOrElse[Int](movieId,1)))
        }.toArray.sortBy(_._2)




  }


  //将更新出来的结果写入数据库中
  def updateRecommends2DB(updatedRecommends: Array[(Int, Double)]):Unit={
    var connection: Connection = null

    var rate=0.6
    try {

      connection=ConnectPoolUtil.getConnection()
      val statement = connection.createStatement()
      for (updatedRecom<-updatedRecommends){
      val resultSet = statement.executeQuery("insert into table_update values('"+updatedRecom._1+"','"+updatedRecom._2+"')")
      }
    } catch {
      case e => e.printStackTrace
      //case _: Throwable => println("ERROR")
    }
    //connection.close()
    connection.close()



  }

  /*def updateRecommends2MongoDB(collection: MongoCollection, newRecommends: Array[(Int, Double)], userId: Int, startTimeMillis: Long): Boolean = {
    //function feature: 将备选电影的预期评分回写到MONGODB中
    val endTimeMillis = System.currentTimeMillis()
    /*
    val query = MongoDBObject("userId" -> userId)
    val setter = $set("recommending" -> newRecommends.map(item => item._1.toString + "," + item._2.toString).mkString("|"),
    "timedelay" -> (System.currentTimeMillis() - startTimeMillis).toDouble / 1000)
    collection.update(query, setter, upsert = true, multi = false)
    */
    val toInsert = MongoDBObject("userId" -> userId,
      "recommending" -> newRecommends.map(item => item._1.toString + "," + item._2.toString).mkString("|"),
      "timedelay" -> (endTimeMillis - startTimeMillis).toDouble / 1000)
    collection.insert(toInsert)
    true
  }
  *
  * */



  def main(args: Array[String]): Unit = {
    val zkServers = "192.168.1.200:2181,192.168.1.201:2181"
    val msgConsumerGroup="msgConsumerGroup1"


    //生成5个DStream
    val dataDStreams = (1 to 5).map{i =>
      KafkaUtils.createStream(ssc, zkServers, msgConsumerGroup, Map("netflix-recommending-system-topic" -> 3), StorageLevel.MEMORY_ONLY)}
    //将上面的5个DStream合并为一个DStream
    var unifiedStream = ssc.union(dataDStreams).map(_._2)

    val dataDStream = unifiedStream.map{ case msgLine =>
      val dataArr: Array[String] = msgLine.split(",")
      val userId = dataArr(0).toInt
      val movieId = dataArr(1).toInt
      val rate = dataArr(2).toDouble
      val startTimeMillis = dataArr(3).toLong
      (userId, movieId, rate, startTimeMillis)
    }.cache


    dataDStream.foreachRDD(rdd => {
      if (!rdd.isEmpty) {
        rdd.map{ case (userId, movieId, rate, startTimeMillis) =>
          //获取近期评分记录
          val recentRatings = getUserRecentRatings(5,userId = userId)
          //获取备选电影
          val candidateMovies = getSimilarMovies(Array(movieId))
          //为备选电影推测评分结果
          val updatedRecommends = createUpdatedRatings(recentRatings, candidateMovies)
          updateRecommends2DB(updatedRecommends)
        }.count()
      }
    })
    println("推荐结果为：--------------------")
    dataDStream.print()
   /* println(getSimilarityBetween2Movies(1,2))*/

    ssc.start()
    ssc.awaitTermination()






  }

}
