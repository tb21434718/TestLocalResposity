import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MovieRecommend0 {
  val fSimilarityThreshold : Float = 0.4f//两部电影之间的相似度大于该值，则被认为是相似电影
  val fScoreThreshold : Float = 7.0f//评分大于该值，则被认为是评分较高
  val nRecommendNum : Int = 10//给用户推荐电影的数量
  val fDegradeFactor : Float = 0.9f//上一次推荐电影的优先级应进行衰减，以免反复推荐
  //创建数据库连接
  var connection=ConnectPoolUtil.getConnection()
  val statement = connection.createStatement


  def main(args: Array[String]): Unit = {
    println("dataddddddd")
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val sparkConf = new SparkConf().setAppName("MovieRecommend").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint("d://ck2")
    val zkServers = "192.168.133.130:2181"
    val msgConsumerGroup="g2"
    val streammessages=KafkaUtils.createStream(ssc, zkServers, msgConsumerGroup,"test3".split(",").map((_, 2)).toMap)
    val dstream=streammessages.map(_._2).map{
    case  line=>
        val dataArr: Array[String] = line.split(",")
        val userId = dataArr(0).toInt
        val movieId = dataArr(1).toInt
        val rate = dataArr(2).toFloat
        val startTimeMillis = dataArr(3).toLong
        (userId, movieId, rate, startTimeMillis)
      //  println(userId, movieId, rate, startTimeMillis)
    }.cache()


    val recentrate=Array(Tuple2(26898747, 3.0f), Tuple2(26614082, 7.7f),Tuple2(26608227, 6.2f),Tuple2(26717795, 8.6f),Tuple2(26801052, 7.7f),Tuple2(26608206, 3.7f),Tuple2(26776531, 5.7f),Tuple2(26828285, 7.7f),Tuple2(26775933, 6.7f) , Tuple2(26663086, 8.5f))
    //得到上次推荐电影列表
    val lastrecommendmovie=Array(Tuple2(26425072, 4.5f), Tuple2(3914513, 1.7f),Tuple2(26752852, 5.3f),Tuple2(26990609, 4.4f),Tuple2(26799731, 2.7f),Tuple2(27059130, 3.7f),Tuple2(26661191, 5.7f),Tuple2(27024903, 4.7f),Tuple2(26863778, 1.7f) , Tuple2(26363254, 2.5f))

    //对每条记录进行操作
    dstream.foreachRDD(rdd=>
      rdd.foreachPartition(partition=>{
      //  val connection =ConnectPoolUtil.getConnection()
        //最近几次评分，应该是通过函数得到

        partition.foreach(record=>{
          val t=getRecommendedMovie(recentrate, lastrecommendmovie)
          var s:String =""
          for(i<-0 until 10)
            {
              s+=t(i)._1+":"+t(i)._2+","
            }
          println(record)
          println("insert into tenscore values("+record._1+",'"+s+"')")
connection.createStatement().execute("update tenscore set movieid_level= '"+s+"' where userid="+record._1)
          //println("insert into tenscore values("+record._1+","+s+")")
        })
      })
    )





    ssc.start()
    ssc.awaitTermination()

/*
    val arrUserMark = Array(Tuple2(26898747, 6.7f), Tuple2(26614082, 7.7f),Tuple2(26608227, 6.2f),Tuple2(26717795, 8.6f),Tuple2(26801052, 7.7f),Tuple2(26608206, 3.7f),Tuple2(26776531, 5.7f),Tuple2(26828285, 7.7f),Tuple2(26775933, 6.7f) , Tuple2(26663086, 8.5f))
    val arrLastTimeRecommendation = Array(Tuple2(26425072, 4.5f), Tuple2(3914513, 1.7f),Tuple2(26752852, 5.3f),Tuple2(26990609, 4.4f),Tuple2(26799731, 2.7f),Tuple2(27059130, 3.7f),Tuple2(26661191, 5.7f),Tuple2(27024903, 4.7f),Tuple2(26863778, 1.7f) , Tuple2(26363254, 2.5f))
    val b = getRecommendedMovie(arrUserMark, arrLastTimeRecommendation)
    for (i <- 0 until nRecommendNum) {
      println(b(i))
    }*/
    //关闭连接，释放资源
   // connection.close
  }

  //获取与电影K相似的电影
  def getMostSimilarKMovie(nMovieId : Int) : Array[Tuple2[Int, Float]] = {
    var arrayMostSimilarKMovie : Array[Tuple2[Int, Float]] = new Array[Tuple2[Int, Float]](nRecommendNum)
    var k : Int = 0
    val rs = statement.executeQuery("select * from moviesimilarity where id1 = " + nMovieId + " order by similarity desc limit " + nRecommendNum.toString)
    //取出查询结果
    while (rs.next) {
      arrayMostSimilarKMovie(k) = Tuple2(rs.getInt("id2"), rs.getFloat("similarity"))
      k += 1
    }
    return arrayMostSimilarKMovie
  }

  //向用户推荐电影
  //入参：用户近期评分（至少一条评分），上次推荐结果（可为空）
  def getRecommendedMovie(arrUserMark : Array[Tuple2[Int, Float]], arrLastTimeRecommendation : Array[Tuple2[Int, Float]]) : Array[Tuple2[Int, Float]] = {
    var arrThisTimeRecommendation : Array[Tuple2[Int, Float]] = new Array[Tuple2[Int, Float]](nRecommendNum)//存储本次推荐结果作为函数返回
    var arrMostSimilarKMovie = getMostSimilarKMovie(arrUserMark(0)._1)//找出与最近一次评过分的电影最相似的nRecommendNum部电影
    var arrMoviePriority : Array[Tuple2[Int, Float]] = new Array[Tuple2[Int, Float]](nRecommendNum)//储存最相似电影的优先级
    //计算最相似电影的优先级
    for (i <- 0 until nRecommendNum) {
      arrMoviePriority(i) = Tuple2(arrMostSimilarKMovie(i)._1, getMoviePriority(arrMostSimilarKMovie(i)._1, arrUserMark))
    }
    //有上次推荐的记录
    if (arrLastTimeRecommendation != null) {
      //与上次推荐结果合并
      var listRecommendation : List[Tuple2[Int, Float]] = List()
      for (i <- 0 until nRecommendNum) {
        listRecommendation = listRecommendation.++(List(arrMoviePriority(i)))
        //listRecommendation = listRecommendation.++(List(arrLastTimeRecommendation(i)))
        listRecommendation = listRecommendation.++(List(Tuple2(arrLastTimeRecommendation(i)._1, arrLastTimeRecommendation(i)._2 * fDegradeFactor)))//上次推荐结果进行时间衰减
      }
      listRecommendation = listRecommendation.sortBy( x => x._2)
      for (i <- nRecommendNum until 2 * nRecommendNum ) {
        arrThisTimeRecommendation(i - nRecommendNum) = listRecommendation(i)
      }
    }
    //没有上次推荐记录
    else {
      arrThisTimeRecommendation = arrMoviePriority
    }
    return arrThisTimeRecommendation
  }

  //计算电影推荐优先级
  def getMoviePriority(nMovieId : Int, arrUserMark : Array[Tuple2[Int, Float]]) : Float = {
    var arrMovieSimilarity : Array[Float] = new Array[Float](arrUserMark.length)
    var arrMovieScore : Array[Float] = new Array[Float](arrUserMark.length)
    for (i <- 0 until arrMovieSimilarity.length) {
      var rs = statement.executeQuery("select * from moviesimilarity where id1 = " + nMovieId +" and id2 = " + arrUserMark(i)._1)
      while (rs.next) {
        arrMovieSimilarity(i) = rs.getFloat("similarity")
      }
    }
    //查询电影评分
    for (i <- 0 until arrMovieScore.length) {
      var rs = statement.executeQuery("select * from movies where id = " + arrUserMark(i)._1)
      while (rs.next) {
        arrMovieScore(i) = rs.getFloat("score")
      }
    }
    //计算推荐优先级
    var fMoviePriority : Float = 0.0f
    var nSimilarityNum : Int = 0
    var nIncount : Int = 0
    var nRecount : Int = 0
    for (i <- 0 until arrUserMark.length) {
      if (arrMovieSimilarity(i) >= fSimilarityThreshold) {
        fMoviePriority += arrMovieScore(i) * arrMovieSimilarity(i)
        nSimilarityNum += 1
        if (arrMovieScore(i) >= fScoreThreshold) {
          nIncount += 1
        }
        else {
          nRecount += 1
        }
      }
    }
    if (nSimilarityNum == 0) {
      fMoviePriority = 0
    }
    else {
      fMoviePriority /= nSimilarityNum.toFloat
    }
    fMoviePriority += Math.log10(Math.max(nIncount, 1)).toFloat
    fMoviePriority -= Math.log10(Math.max(nRecount, 1)).toFloat
    return fMoviePriority
  }

}
