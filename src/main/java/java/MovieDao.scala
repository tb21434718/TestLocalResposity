import scala.collection.mutable.ListBuffer
object MovieDao {
var connection=ConnectPoolUtil.getConnection()
  var statement=connection.createStatement()
  def save(list:ListBuffer[Tuple2[Int, Float]]): Unit ={
    for(e<-list) {
      statement.execute("insert into ")
    }
  }

}
