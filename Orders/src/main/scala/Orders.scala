import org.apache.spark.rdd.JdbcRDD
import java.sql.{Connection, DriverManager, ResultSet, Timestamp}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.secretsales.analytics.RedShift

object Orders {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Orders")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // try to load config ./config/ordes.sh
    val mysqlUrl = sys.env("MYSQL_HOST")
    val mysqlUser = sys.env("MYSQL_USER")
    val mysqlPassword = sys.env("MYSQL_PASS")
    val redshiftUrl = sys.env("REDSHIFT_HOST")
    val redshiftUser = sys.env("REDSHIFT_USER")
    val redshiftPassword = sys.env("REDSHIFT_PASS")
    val AWSKey = sys.env("AWS_ACCESS_KEY_ID")
    val AWSSecret = sys.env("AWS_SECRET_ACCESS_KEY")

    val timestamp: Long = System.currentTimeMillis / 1000
    val S3Path = "secretsales-analytics/RedShift/Load/Orders2/"+timestamp

    val ordersLastMin = new JdbcRDD(sc,
      () => DriverManager.getConnection(mysqlUrl,mysqlUser,mysqlPassword),
      "select od.order_detail_id, o.order_id, od.product_id, p.collection_id, c.name as 'colection_name', o.total_price as 'order_total', od.price, p.store_price as 'cost_price', o.discount, o.discountcode, o.delivery_price, o.order_progress_id, op.order_progress_name, o.vat, o.vat_value, o.added as 'created' from orders o LEFT JOIN order_progress op ON o.order_progress_id = op.order_progress_id LEFT JOIN order_details od ON o.order_id = od.order_id LEFT JOIN products p ON od.product_id = p.id LEFT JOIN collections c ON p.collection_id = c.id where o.added BETWEEN DATE_FORMAT(DATE_SUB(CURTIME(), INTERVAL 1 MINUTE), '%Y-%m-%d %H:%i:00') AND DATE_FORMAT(DATE_SUB(CURTIME(), INTERVAL 1 MINUTE), '%Y-%m-%d %H:%i:59') LIMIT ?, ?",
      0, 999, 10, r => Row(
        r.getInt("order_detail_id"),
        r.getInt("order_id"),
        r.getInt("product_id"),
        r.getInt("collection_id"),
        r.getString("colection_name"),
        r.getFloat("cost_price"),
        r.getFloat("price"),
        (r.getFloat("price")/r.getFloat("order_total")).toFloat*r.getFloat("discount"),
        r.getString("discountcode"),
        (r.getFloat("price")/r.getFloat("order_total")).toFloat*r.getFloat("delivery_price"),
        r.getInt("order_progress_id"),
        r.getString("order_progress_name"),
        r.getFloat("vat"),
        (r.getFloat("price")/100.0f).toFloat*r.getFloat("vat"),
        r.getTimestamp("created")
      )
    )



    // create schema map
    val schema = StructType(Array(
      StructField("order_detail_id",IntegerType,true),
      StructField("order_id",IntegerType,true),
      StructField("product_id",IntegerType,true),
      StructField("collection_id",IntegerType,true),
      StructField("colection_name",StringType,true),
      StructField("cost_price",FloatType,true),
      StructField("price",FloatType,true),
      StructField("discount",FloatType,true),
      StructField("discountcode",StringType,true),
      StructField("delivery_price",FloatType,true),
      StructField("order_progress_id",IntegerType,true),
      StructField("order_progress_name",StringType,true),
      StructField("vat",FloatType,true),
      StructField("vat_value",FloatType,true),
      StructField("created",TimestampType,true)
    ))

    // convert to DataFrame
    val DF = sqlContext.createDataFrame(ordersLastMin, schema)
    DF.write
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .save("s3n://" + S3Path)

    val RedShift = new RedShift(redshiftUrl, redshiftUser, redshiftPassword, AWSKey, AWSSecret)
    RedShift.CopyCSVFromS3("orders3", S3Path+"/part-")
  }
}
