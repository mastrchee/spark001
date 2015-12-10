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
  val applicationId = sparkContext.applicationId
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

    val table = "orders"
    val tableUniqueKey = "order_id"
    val tableLastUpdatedKey = "updated_at"
    val S3Path = "secretsales-analytics/RedShift/Load/"+table+"/"+applicationId

    val tmp = sqlContext.read.format("jdbc").options(Map(
        ("url", redshiftHost),
        ("user", redshiftUser),
        ("password", redshiftPassword),
        ("dbtable","(select max("+tableUniqueKey+") as last_id, max(updated) as last_updated from "+table+") tmp"),
        ("driver", "com.amazon.redshift.jdbc41.Driver")
      )).load()

    val row = tmp.select("last_id", "last_updated").first();
    val tableLastId = row.getLong(0)
    val tableLastUpdated = if (row.getTimestamp(1) == null) "0000-00-00 00:00:00" else row.getTimestamp(1).toString

    // get data
    val data = new JdbcRDD(sparkContext,
      () => DriverManager.getConnection(mysqlHost, mysqlUser, mysqlPassword),
      "SELECT `order_id`, `user_id`, `total_price`, `discountcode`, `delivery_method`, `delivery_price`, left(`VendorTxCode`, 2) as 'payment_method', `VendorTxCode`, `order_progress_id`, `added`, `updated_at` FROM orders WHERE order_id > "+tableLastId+" OR updated_at > '"+tableLastUpdated+"' LIMIT ?, ?",
      0, 10000, 100, r => Row(
        r.getInt("order_id"),
        r.getInt("user_id"),
        if (r.getString("VendorTxCode") == null) "" else r.getString("VendorTxCode"),
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
        if (r.getString("added") == null) "" else r.getString("added"),
        if (r.getString("updated_at") == null) "" else r.getString("updated_at")
      )
    )

    // create schema map
    val schema = StructType(Array(
      StructField("order_id",IntegerType,true),
      StructField("user_id",IntegerType,true),
      StructField("checkout_id",StringType,true),
      StructField("total_price",FloatType,true),
      StructField("discountcode",StringType,true),
      StructField("delivery_price",FloatType,true),
      StructField("order_progress_id",IntegerType,true),
      StructField("payment_method",StringType,true),
      StructField("created",StringType,true),
      StructField("updated",StringType,true)
    ))

    // convert to DataFrame
    val DF = sqlContext.createDataFrame(data, schema)

    // copy to redshift
    val RedShift = new RedShift(redshiftHost, redshiftUser, redshiftPassword, awsKey, awsSecret, applicationId + "_staging_")
    RedShift.CopyFromDataFrame(DF, table, S3Path, tableUniqueKey)
  }
}
