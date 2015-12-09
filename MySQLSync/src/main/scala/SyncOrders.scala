import org.apache.spark.rdd.JdbcRDD
import java.sql.{Connection, DriverManager, ResultSet, Timestamp}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.secretsales.analytics.RedShift

object SyncOrders {
  val sparkConf = new SparkConf()
  val sparkContext = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sparkContext)
  import sqlContext.implicits._

  def main(args: Array[String]) {
    val mysqlHost = System.getenv("MYSQL_HOST")
    val mysqlUser = System.getenv("MYSQL_USER")
    val mysqlPassword = System.getenv("MYSQL_PASS")
    val redshiftHost = System.getenv("REDSHIFT_HOST")
    val redshiftUser = System.getenv("REDSHIFT_USER")
    val redshiftPassword = System.getenv("REDSHIFT_PASS")
    val awsKey = System.getenv("AWS_ACCESS_KEY_ID")
    val awsSecret = System.getenv("AWS_SECRET_ACCESS_KEY")

    val redshiftTable = "orders"
    val S3Path = "secretsales-analytics/RedShift/Load/"+redshiftTable+"/"+(System.currentTimeMillis / 1000)

    val tmp = sqlContext.read.format("jdbc").options(Map(
        ("url", redshiftHost),
        ("user", redshiftUser),
        ("password", redshiftPassword),
        ("dbtable","(select max(order_id) as max_id from "+redshiftTable+") tmp"),
        ("driver", "com.amazon.redshift.jdbc41.Driver")
      )).load()

      val maxId=tmp.select("max_id").first().getLong(0)

    // get data
    val orders = new JdbcRDD(sparkContext,
      () => DriverManager.getConnection(mysqlHost, mysqlUser, mysqlPassword),
      "SELECT `order_id`, `user_id`, `total_price`, `discountcode`, `delivery_method`, `delivery_price`, left(`VendorTxCode`, 2) as 'payment_method', `order_progress_id`, `added` FROM orders WHERE order_id > "+maxId+" LIMIT ?, ?",
      0, 999, 2, r => Row(
        r.getInt("order_id"),
        r.getInt("user_id"),
        r.getFloat("total_price"),
        r.getString("discountcode"),
        r.getFloat("delivery_price"),
        r.getInt("order_progress_id"),
        r.getString("payment_method") match {
          case "AM" => "Amazon"
          case "PP" => "PayPal"
          case "OO" => "Ogone"
          case "AY" => "Adyen"
          case _    => "Other"
        },
        r.getTimestamp("added")
      )
    )

    // create schema map
    val schema = StructType(Array(
      StructField("order_id",IntegerType,true),
      StructField("user_id",IntegerType,true),
      StructField("total_price",FloatType,true),
      StructField("discountcode",StringType,true),
      StructField("delivery_price",FloatType,true),
      StructField("order_progress_id",IntegerType,true),
      StructField("payment_method",StringType,true),
      StructField("created",TimestampType,true)
    ))

    // convert to DataFrame
    val DF = sqlContext.createDataFrame(orders, schema)

    // copy to redshift
    val RedShift = new RedShift(redshiftHost, redshiftUser, redshiftPassword, awsKey, awsSecret)
    RedShift.CopyFromDataFrame(DF, redshiftTable, S3Path)
  }
}
