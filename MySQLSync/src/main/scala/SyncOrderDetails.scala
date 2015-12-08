import org.apache.spark.rdd.JdbcRDD
import java.sql.{Connection, DriverManager, ResultSet, Timestamp}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.secretsales.analytics.RedShift

object SyncOrderDetails {
  val sparkConf = new SparkConf()
  val sparkContext = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sparkContext)
  import sqlContext.implicits._

  def main(args: Array[String]) {
    val mysqlHost = System.getenv("MYSQL_HOST")
    val mysqlUser = System.getenv("MYSQL_USER")
    val mysqlPassword = sys.env("MYSQL_PASS")
    val redshiftHost = sys.env("REDSHIFT_HOST")
    val redshiftUser = sys.env("REDSHIFT_USER")
    val redshiftPassword = sys.env("REDSHIFT_PASS")
    val awsKey = sys.env("AWS_ACCESS_KEY_ID")
    val awsSecret = sys.env("AWS_SECRET_ACCESS_KEY")

    val redshiftTable = "order_details"
    val S3Path = "secretsales-analytics/RedShift/Load/"+redshiftTable+"/"+(System.currentTimeMillis / 1000)

    // get data
    val ordersLastMin = new JdbcRDD(sparkContext,
      () => DriverManager.getConnection(mysqlHost, mysqlUser, mysqlPassword),
      "select od.order_detail_id, o.order_id, od.product_id, p.collection_id, o.total_price as 'order_total', od.price, p.store_price as 'cost_price', o.discount, o.vat, o.vat_value, o.added from orders o LEFT JOIN order_details od ON o.order_id = od.order_id LEFT JOIN products p ON od.product_id = p.id where o.added BETWEEN DATE_FORMAT(DATE_SUB(CURTIME(), INTERVAL 6 MINUTE), '%Y-%m-%d %H:%i:00') AND DATE_FORMAT(DATE_SUB(CURTIME(), INTERVAL 1 MINUTE), '%Y-%m-%d %H:%i:59') ORDER BY od.order_detail_id ASC LIMIT ?, ?",
      0, 999, 10, r => Row(
        r.getInt("order_detail_id"),
        r.getInt("order_id"),
        r.getInt("product_id"),
        r.getInt("collection_id"),
        r.getFloat("cost_price"),
        r.getFloat("price"),
        (r.getFloat("price")/r.getFloat("order_total")).toFloat*r.getFloat("discount"), // disount per product
        (r.getFloat("price")/100.0f).toFloat*r.getFloat("vat"), // vat_value per product
        r.getTimestamp("added")
      )
    )

    // create schema map
    val schema = StructType(Array(
      StructField("order_detail_id",IntegerType,true),
      StructField("order_id",IntegerType,true),
      StructField("product_id",IntegerType,true),
      StructField("collection_id",IntegerType,true),
      StructField("cost_price",FloatType,true),
      StructField("price",FloatType,true),
      StructField("discount",FloatType,true),
      StructField("vat_value",FloatType,true),
      StructField("created",TimestampType,true)
    ))

    // convert to DataFrame
    val DF = sqlContext.createDataFrame(ordersLastMin, schema)

    // copy to redshift
    val RedShift = new RedShift(redshiftHost, redshiftUser, redshiftPassword, awsKey, awsSecret)
    RedShift.CopyFromDataFrame(DF, redshiftTable, S3Path)
  }
}
