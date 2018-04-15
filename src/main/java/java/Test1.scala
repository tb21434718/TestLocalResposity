import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Test1 {
  def main(args: Array[String]): Unit = {
    //屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)
    val sparkConf = new SparkConf().setAppName("Mytest").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val t = sc.parallelize(Array(Tuple2(2, 3), Tuple2(4, 5), Tuple2(6, 7)))
    println(t.map(x=>x._1+x._2))
  }
}