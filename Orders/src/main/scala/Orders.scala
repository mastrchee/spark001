import org.apache.spark.rdd.JdbcRDD
import java.sql.{Connection, DriverManager, ResultSet, Timestamp}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import scala.collection.mutable.MutableList

object Orders {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Orders")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // try to load config
    val mysqlUrl = sys.env("MYSQL_HOST")
    val mysqlUser = sys.env("MYSQL_USER")
    val mysqlPassword = sys.env("MYSQL_PASS")
    val redshiftUrl = sys.env("REDSHIFT_HOST")
    val redshiftUser = sys.env("REDSHIFT_USER")
    val redshiftPassword = sys.env("REDSHIFT_PASS")

    val ordersLastMin = new JdbcRDD(sc,
      () => DriverManager.getConnection(mysqlUrl,mysqlUser,mysqlPassword),
      "select order_id, total_price, discount, discountcode, delivery_method, delivery_price, order_progress_id, vat, vat_value, source, added from orders where added BETWEEN DATE_FORMAT(DATE_SUB(CURTIME(), INTERVAL 1 MINUTE), '%Y-%m-%d %H:%i:00') AND DATE_FORMAT(DATE_SUB(CURTIME(), INTERVAL 1 MINUTE), '%Y-%m-%d %H:%i:59') LIMIT ?, ?",
      0, 999, 10, r => Map(
        ("order_id", r.getInt("order_id")),
        ("total_price", r.getFloat("total_price")),
        ("discount", r.getFloat("discount")),
        ("discount_code", r.getString("discountcode")),
        ("delivery_method", r.getString("delivery_method")),
        ("delivery_price", r.getFloat("delivery_price")),
        ("order_progress_id", r.getInt("order_progress_id")),
        ("vat", r.getFloat("vat")),
        ("vat_value", r.getFloat("vat_value")),
        ("source", r.getString("source")),
        ("added", r.getString("added"))
      )
    )

    var processed = MutableList[Int]()
    for (order <- ordersLastMin) {
      println("Processing OrderId: " + order("order_id"))

      if (processed.indexOf(order("order_id")) > 0) {
        println(" Order already processed")
      } else {
        try {
          var redshift = DriverManager.getConnection(redshiftUrl,redshiftUser,redshiftPassword)
          var delete = redshift.prepareStatement("DELETE FROM orders2 WHERE order_id = ?")
          delete.setInt(1, order("order_id").toString.toInt)
          delete.executeUpdate()

          val insert = redshift.prepareStatement("INSERT INTO orders2 VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
          var timestamp = Timestamp.valueOf(order("added").toString)
          insert.setInt(1, order("order_id").toString.toInt)
          insert.setFloat(2, order("total_price").toString.toFloat)
          insert.setFloat(3, order("discount").toString.toFloat)
          insert.setString(4, order("discount_code").toString)
          insert.setString(5, order("delivery_method").toString)
          insert.setFloat(6, order("delivery_price").toString.toFloat)
          insert.setInt(7, order("order_progress_id").toString.toInt)
          insert.setFloat(8, order("vat").toString.toFloat)
          insert.setFloat(9, order("vat_value").toString.toFloat)
          insert.setString(10, order("source").toString)
          insert.setTimestamp(11, timestamp)
          insert.executeUpdate()
          processed += order("order_id").toString.toInt
        } catch {
          case _: Throwable => println("Some error happened")
        }
      }
    }
  }
}
