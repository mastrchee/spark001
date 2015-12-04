import org.apache.spark.rdd.JdbcRDD
import java.sql.{Connection, DriverManager, ResultSet, Timestamp}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._

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
      "select od.order_detail_id, o.order_id, od.product_id, p.collection_id, c.name as 'colection_name', o.total_price as 'order_total', od.price, p.store_price as 'cost_price', o.discount, o.discountcode, o.delivery_price, o.order_progress_id, op.order_progress_name, o.vat, o.vat_value, o.added as 'created' from orders o LEFT JOIN order_progress op ON o.order_progress_id = op.order_progress_id LEFT JOIN order_details od ON o.order_id = od.order_id LEFT JOIN products p ON od.product_id = p.id LEFT JOIN collections c ON p.collection_id = c.id where o.added BETWEEN DATE_FORMAT(DATE_SUB(CURTIME(), INTERVAL 1 MINUTE), '%Y-%m-%d %H:%i:00') AND DATE_FORMAT(DATE_SUB(CURTIME(), INTERVAL 1 MINUTE), '%Y-%m-%d %H:%i:59') LIMIT ?, ?",
      0, 999, 10, r => Map(
        ("order_detail_id", r.getInt("order_detail_id")),
        ("order_id", r.getInt("order_id")),
        ("product_id", r.getInt("product_id")),
        ("collection_id", r.getInt("collection_id")),
        ("colection_name", r.getString("colection_name")),
        ("order_total", r.getFloat("order_total")),
        ("cost_price", r.getFloat("cost_price")),
        ("price", r.getFloat("price")),
        ("discount", r.getFloat("discount")),
        ("discountcode", r.getString("discountcode")),
        ("delivery_price", r.getFloat("delivery_price")),
        ("order_progress_id", r.getInt("order_progress_id")),
        ("order_progress_name", r.getString("order_progress_name")),
        ("vat", r.getFloat("vat")),
        ("vat_value", r.getFloat("vat_value")),
        ("created", r.getString("created"))
      )
    )

    for (order <- ordersLastMin) {
        var redshift = DriverManager.getConnection(redshiftUrl,redshiftUser,redshiftPassword)
        var delete = redshift.prepareStatement("DELETE FROM orders3 WHERE order_detail_id = ?")
        delete.setInt(1, order("order_detail_id").toString.toInt)
        delete.executeUpdate()

        val insert = redshift.prepareStatement("INSERT INTO orders3 VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        var timestamp = Timestamp.valueOf(order("created").toString)
        var vat = order("vat").toString.toFloat
        var price = order("price").toString.toFloat
        var vatValue = (price/100.0f).toFloat*vat

        var orderTotal = order("order_total").toString.toFloat
        var totalDiscount = order("discount").toString.toFloat
        var discount = (price/orderTotal).toFloat*totalDiscount

        var totalDeliveryPrice = order("delivery_price").toString.toFloat
        var deliveryPrice = (price/orderTotal).toFloat*totalDeliveryPrice

        insert.setInt(1, order("order_detail_id").toString.toInt)
        insert.setInt(2, order("order_id").toString.toInt)
        insert.setInt(3, order("product_id").toString.toInt)
        insert.setInt(4, order("collection_id").toString.toInt)
        insert.setString(5, order("colection_name").toString)
        insert.setFloat(6, order("cost_price").toString.toFloat)
        insert.setFloat(7, order("price").toString.toFloat)
        insert.setFloat(8, discount)
        insert.setString(9, order("discountcode").toString)
        insert.setFloat(10, order("delivery_price").toString.toFloat)
        insert.setInt(11, order("order_progress_id").toString.toInt)
        insert.setString(12, order("order_progress_name").toString)
        insert.setFloat(13, order("vat").toString.toFloat)
        insert.setFloat(14, vatValue)
        insert.setTimestamp(15, timestamp)
        insert.executeUpdate()
    }
  }
}
