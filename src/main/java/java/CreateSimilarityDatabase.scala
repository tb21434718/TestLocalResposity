import com.mysql.jdbc.Driver
import org.apache.spark.{SparkConf, SparkContext, sql}

import scala.collection.mutable
import scala.util.control._
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SQLContext
import java.sql.{Connection,DriverManager,PreparedStatement}

object CreateSimilarityDatabase {
  def main(args: Array[String]) {
    // 访问本地MySQL服务器，通过3306端口访问mysql数据库，数据库名为moviedata
    val url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf-8&useSSL=false"
    //驱动名称
    val driver = "com.mysql.jdbc.Driver"
    //用户名
    val username = "root"
    //密码
    val password = "root"
    //初始化数据连接
    var connection: Connection=null
    try {
      //注册Driver
      Class.forName(driver)
      //得到连接
      connection = DriverManager.getConnection(url, username, password)
      connection.setAutoCommit(false);
      val statement = connection.prepareStatement("")
      //执行查询语句，并返回结果
      val rs = statement.executeQuery("SELECT id, name, director, writer, actor, type, releasedate, area, language, score  FROM movies")
      //创建一个链表存储所有电影对象
      var listMovie : List[Movie] = List()
      //链表存储所有电影对象
      while (rs.next) {
        val nId = rs.getInt("id")
        val strName = rs.getString("name")
        val setDirector = getSetFromString(rs.getString("director"), 3)
        val setWriter = getSetFromString(rs.getString("writer"), 3)
        val setActor = getSetFromString(rs.getString("actor"), 5)
        val setType = getSetFromString(rs.getString("type"), 10)
        val nYear = rs.getInt("releasedate")
        val setCountry = getSetFromString(rs.getString("area"), 1)
        val setLanguage = getSetFromString(rs.getString("language"), 1)
        val fScore = rs.getFloat("score")
        val movie = new Movie(nId, strName, setDirector, setWriter, setActor, setType, nYear, setCountry, setLanguage, fScore)
        listMovie = listMovie.:+(movie)
      }
      println("查询数据完成！")

      //创建rdd
      val conf = new SparkConf().setMaster("MoviesRecommend ").setAppName("Collaborative Filtering").setMaster("local[2]")
      val sc = new SparkContext(conf)
      val rdd = sc.parallelize(listMovie)
      val nMovieNum : Int = rdd.count().toInt
      val rddList = rdd.take(nMovieNum)
      println("开始计算电影相似度！")

      var s:StringBuffer=new StringBuffer()
      val pre:String="insert into moviesimilarity(id1, id2, similarity) values"
      var k : Int = 1
      for (i <- 0 until nMovieNum) {
        if(i<nMovieNum-1) {
          for (j <- i + 1 until nMovieNum) {
            val fMovieSimilarity: Float = getMovieSimilarity(rddList(i), rddList(j))
            //  println(k + " / " + nMovieNum * (nMovieNum - 1) / 2)
            val s1 = " (" + rddList(i).nId.toString + ", " + rddList(j).nId.toString + ", " + fMovieSimilarity.toString + "),"
            val s2 = " (" + rddList(j).nId.toString + ", " + rddList(i).nId.toString + ", " + fMovieSimilarity.toString + "),"
            s.append(s1)
            s.append(s2)
            // k += 1
          }
          val sql = pre + s.substring(0, s.length() - 1)
          statement.addBatch(sql);
          // 执行操作
          statement.executeBatch();
          // 提交事务
          connection.commit();
          s = new StringBuffer()
        }

      }
      println("success")
    }catch {
      case e: Exception => e.printStackTrace
    }
    //关闭连接，释放资源
    connection.close
  }

  //电影类
  class Movie(val nIdc : Int, val strNamec : String, val setDirectorc : Set[String], val setWriterc : Set[String], val setActorc : Set[String], val setTypec : Set[String], val nYearc : Int, val setCountryc : Set[String], val setLanguagec : Set[String], val fScorec : Float) extends Serializable {
    var nId : Int = nIdc
    var strName: String = strNamec
    var setDirector : Set[String] = setDirectorc
    var setWriter : Set[String] = setWriterc
    var setActor : Set[String] = setActorc
    var setType : Set[String] = setTypec
    var nYear : Int = nYearc
    var setCountry : Set[String] = setCountryc
    var setLanguage : Set[String] = setLanguagec
    var fScore : Float = fScorec

    def show(): Unit ={
      println(nId + " | " + strName + " | " + setDirector + " | " + setWriter + " | " + setActor + " | " + setType + " | " + nYear + " | " + setCountry + " | " + setLanguage + " | " + fScore)
    }
  }

  //String转Set
  def getSetFromString (stringAttribute: String, nSetMaxLength : Int) : Set[String] = {
    val arrayString = stringAttribute.split("/")
    var setAttribute : mutable.Set[String] = mutable.Set()
    var i : Int = 0
    while (i < nSetMaxLength && i < arrayString.length) {
      setAttribute = setAttribute.+(arrayString(i).replaceAll(" ", ""))
      i += 1
    }
    return setAttribute.toSet
  }

  //计算两部电影的名字属性之间的相似度
  def getMovieNameSimilarity (strMovieName1 : String, strMovieName2 : String) : Float = {
    if (strMovieName1 == "-1" || strMovieName2 == "-1"){
      return 0.0f
    }
    var setMovieName1 : Set[Char] = Set()
    val loop = new Breaks
    loop.breakable {
      for (i <- 0 until strMovieName1.length) {
        if (strMovieName1.charAt(i) == ':' || strMovieName1.charAt(i) == '：' || strMovieName1.charAt(i) == ' ') {
          loop.break
        }
        setMovieName1 = setMovieName1.+(strMovieName1.charAt(i))
      }
    }
    var setMovieName2 : Set[Char] = Set()
    loop.breakable {
      for (i <- 0 until strMovieName2.length) {
        if (strMovieName2.charAt(i) == ':' || strMovieName2.charAt(i) == '：') {
          loop.break
        }
        setMovieName2 = setMovieName2.+(strMovieName2.charAt(i))
      }
    }
    val setMovieName : Set[Char] = setMovieName1 ++ setMovieName2
    val fMovieNameSimilarity : Float = setMovieName1.&(setMovieName2).size.toFloat / setMovieName.size.toFloat
    return fMovieNameSimilarity
  }

  //计算两部电影的导演属性之间的相似度
  def getMovieDirectorSimilarity (setMovieDirector1 : Set[String], setMovieDirector2 : Set[String]) : Float = {
    if (setMovieDirector1.head == "-1" || setMovieDirector2.head == "-1"){
      return 0.0f
    }
    val setMovieDirector : Set[String] = setMovieDirector1 ++ setMovieDirector2
    var fMovieDirectorSimilarity : Float = 0.0f
    if (setMovieDirector.size < setMovieDirector1.size + setMovieDirector2.size) {
      fMovieDirectorSimilarity = 1.0f
    }
    return fMovieDirectorSimilarity
  }

  //计算两部电影的编剧属性之间的相似度
  def getMovieWriterSimilarity (setMovieWriter1 : Set[String], setMovieWriter2 : Set[String]) : Float = {
    if (setMovieWriter1.head == "-1" || setMovieWriter2.head == "-1"){
      return 0.0f
    }
    val setMovieWriter : Set[String] = setMovieWriter1 ++ setMovieWriter2
    var fMovieWriterSimilarity : Float = 0.0f
    if (setMovieWriter.size < setMovieWriter1.size + setMovieWriter2.size) {
      fMovieWriterSimilarity = 1.0f
    }
    return fMovieWriterSimilarity
  }

  //计算两部电影的演员属性之间的相似度
  def getMovieActorSimilarity (setMovieActor1 : Set[String], setMovieActor2 : Set[String]) : Float = {
    if (setMovieActor1.head == "-1" || setMovieActor2.head == "-1"){
      return 0.0f
    }
    val setMovieActor : Set[String] = setMovieActor1 ++ setMovieActor2
    val fMovieActorSimilarity : Float = setMovieActor1.&(setMovieActor2).size.toFloat / setMovieActor.size.toFloat
    return fMovieActorSimilarity
  }

  //计算两部电影的类型属性之间的相似度
  def getMovieTypeSimilarity (setMovieType1 : Set[String], setMovieType2 : Set[String]) : Float = {
    if (setMovieType1.head == "-1" || setMovieType2.head == "-1"){
      return 0.0f
    }
    val setMovieType : Set[String] = setMovieType1 ++ setMovieType2
    val fMovieTypeSimilarity : Float = setMovieType1.&(setMovieType2).size.toFloat / setMovieType.size.toFloat
    return fMovieTypeSimilarity
  }

  //计算两部电影的年份属性之间的相似度
  def getMovieYearSimilarity (nMovieYear1 : Int, nMovieYear2 : Int) : Float = {
    if (nMovieYear1 == -1 || nMovieYear2 == -1){
      return 0.0f
    }
    var fMovieYearSimilarity : Float = 1.0f
    val nYearGap : Int = Math.abs(nMovieYear1 - nMovieYear2)
    if (nYearGap > 5){
      fMovieYearSimilarity = 5.0f / nYearGap.toFloat
    }
    return fMovieYearSimilarity
  }

  //计算两部电影的国家属性之间的相似度
  def getMovieCountrySimilarity (setMovieCountry1 : Set[String], setMovieCountry2 : Set[String]) : Float = {
    if (setMovieCountry1.head == "-1" || setMovieCountry2.head == "-1"){
      return 0.0f
    }
    val setMovieCountry : Set[String] = setMovieCountry1 ++ setMovieCountry2
    val fMovieCountrySimilarity : Float = setMovieCountry1.&(setMovieCountry2).size.toFloat / setMovieCountry.size.toFloat
    return fMovieCountrySimilarity
  }

  //计算两部电影的语言属性之间的相似度
  def getMovieLanguageSimilarity (setMovieLanguage1 : Set[String], setMovieLanguage2 : Set[String]) : Float = {
    if (setMovieLanguage1.head == "-1" || setMovieLanguage2.head == "-1"){
      return 0.0f
    }
    val setMovieLanguage : Set[String] = setMovieLanguage1 ++ setMovieLanguage2
    val fMovieLanguageSimilarity : Float = setMovieLanguage1.&(setMovieLanguage2).size.toFloat / setMovieLanguage.size.toFloat
    return fMovieLanguageSimilarity
  }

  //计算两部电影的评分属性之间的相似度
  def getMovieScoreSimilarity (fMovieScore1 : Float, fMovieScore2 : Float) : Float = {
    if (fMovieScore1 == -1.0f || fMovieScore2 == -1.0f){
      return 0.0f
    }
    val fScoreGap : Float = Math.abs(fMovieScore1 - fMovieScore2)
    var fMovieScoreSimilarity : Float = 1.0f / (1.0f + fScoreGap)
    return fMovieScoreSimilarity
  }

  //计算两部电影之间的相似度
  def getMovieSimilarity (cMovie1 : Movie, cMovie2 : Movie) : Float = {
    val fMovieNameSimilarity : Float = getMovieNameSimilarity(cMovie1.strName, cMovie2.strName)
    val fMovieDirectorSimilarity : Float = getMovieDirectorSimilarity(cMovie1.setDirector, cMovie2.setDirector)
    val fMovieWriterSimilarity : Float = getMovieWriterSimilarity(cMovie1.setWriter, cMovie2.setWriter)
    val fMovieActorSimilarity : Float = getMovieActorSimilarity(cMovie1.setActor, cMovie2.setActor)
    val fMovieTypeSimilarity : Float = getMovieTypeSimilarity(cMovie1.setType, cMovie2.setType)
    val fMovieYearSimilarity : Float = getMovieYearSimilarity(cMovie1.nYear, cMovie2.nYear)
    val fMovieCountrySimilarity : Float = getMovieCountrySimilarity(cMovie1.setCountry, cMovie2.setCountry)
    val fMovieLanguageSimilarity : Float = getMovieLanguageSimilarity(cMovie1.setLanguage, cMovie2.setLanguage)
    val fMovieScoreSimilarity : Float = getMovieScoreSimilarity(cMovie1.fScore, cMovie2.fScore)

    val fMovieNameWeight : Float = 3
    val fMovieDirectorWeight : Float = 3
    val fMovieWriterWeight : Float = 3
    val fMovieActorWeight : Float = 2
    val fMovieTypeWeight : Float = 5
    val fMovieYearWeight : Float = 1
    val fMovieCountryWeight : Float = 5
    val fMovieLanguageWeight : Float = 5
    val fMovieScoreWeight : Float = 1
    val fMovieTotalWeight : Float = fMovieNameWeight + fMovieDirectorWeight + fMovieWriterWeight + fMovieActorWeight + fMovieTypeWeight + fMovieYearWeight + fMovieCountryWeight + fMovieLanguageWeight + fMovieScoreWeight

    var fMovieSimilarity : Float = 0.0f
    fMovieSimilarity += fMovieNameSimilarity * fMovieNameWeight
    fMovieSimilarity += fMovieDirectorSimilarity * fMovieDirectorWeight
    fMovieSimilarity += fMovieWriterSimilarity * fMovieWriterWeight
    fMovieSimilarity += fMovieActorSimilarity * fMovieActorWeight
    fMovieSimilarity += fMovieTypeSimilarity * fMovieTypeWeight
    fMovieSimilarity += fMovieYearSimilarity * fMovieYearWeight
    fMovieSimilarity += fMovieCountrySimilarity * fMovieCountryWeight
    fMovieSimilarity += fMovieLanguageSimilarity * fMovieLanguageWeight
    fMovieSimilarity += fMovieScoreSimilarity * fMovieScoreWeight
    fMovieSimilarity /= fMovieTotalWeight
/*
    println("fMovieNameSimilarity = " + fMovieNameSimilarity)
    println("fMovieDirectorSimilarity = " + fMovieDirectorSimilarity)
    println("fMovieWriterSimilarity = " + fMovieWriterSimilarity)
    println("fMovieActorSimilarity = " + fMovieActorSimilarity)
    println("fMovieTypeSimilarity = " + fMovieTypeSimilarity)
    println("fMovieYearSimilarity = " + fMovieYearSimilarity)
    println("fMovieCountrySimilarity = " + fMovieCountrySimilarity)
    println("fMovieLanguageSimilarity = " + fMovieLanguageSimilarity)
    println("fMovieScoreSimilarity = " + fMovieScoreSimilarity)
    println("fMovieSimilarity = " + fMovieSimilarity)
*/
    return fMovieSimilarity
  }
}