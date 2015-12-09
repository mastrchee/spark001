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
    val mysqlPassword = System.getenv("MYSQL_PASS")
    val redshiftHost = System.getenv("REDSHIFT_HOST")
    val redshiftUser = System.getenv("REDSHIFT_USER")
    val redshiftPassword = System.getenv("REDSHIFT_PASS")
    val awsKey = System.getenv("AWS_ACCESS_KEY_ID")
    val awsSecret = System.getenv("AWS_SECRET_ACCESS_KEY")

    val table = "order_details"
    val tableKey = "order_detail_id"
    val S3Path = "secretsales-analytics/RedShift/Load/"+table+"/"+(System.currentTimeMillis / 1000)

    val tmp = sqlContext.read.format("jdbc").options(Map(
        ("url", redshiftHost),
        ("user", redshiftUser),
        ("password", redshiftPassword),
        ("dbtable","(select max("+tableKey+") as max_id from "+table+") tmp"),
        ("driver", "com.amazon.redshift.jdbc41.Driver")
      )).load()

    val maxId=tmp.select("max_id").first().getLong(0)

    // val data = sqlContext.read.format("jdbc").options(Map(
    //     ("url", mysqlHost),
    //     ("user", mysqlUser),
    //     ("password", mysqlPassword),
    //     ("dbtable", "()" + data),
    //     ("driver", "com.mysql.jdbc.Driver")
    //   )).load()

    // get data
    val data = new JdbcRDD(sparkContext,
      () => DriverManager.getConnection(mysqlHost, mysqlUser, mysqlPassword),
      "SELECT od.order_detail_id, o.order_id, od.product_id, IFNULL(p.collection_id, 0) as 'collection_id', o.total_price, od.price, IFNULL(p.store_price, 0) as 'cost_price', o.discount, o.vat, o.vat_value, o.added FROM order_details od INNER JOIN orders o ON od.order_id = o.order_id LEFT JOIN products p ON od.product_id = p.id WHERE od.order_detail_id > "+maxId+" LIMIT ?, ?",
      0, 10000, 100, r => Row(
        r.getInt("order_detail_id"),
        r.getInt("order_id"),
        r.getInt("product_id"),
        r.getInt("collection_id"),
        r.getFloat("cost_price"),
        r.getFloat("price"),
        if (r.getFloat("discount") > 0) (r.getFloat("price")/r.getFloat("total_price")).toFloat*r.getFloat("discount") else 0.0f, // disount per product (ternary check because of s**tty data)
        if (r.getFloat("vat") > 0) (r.getFloat("price")/(100.0f+r.getFloat("vat"))) * r.getFloat("vat") else 0.0f, // vat_value per product (ternary check because of s**tty data)
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
    val DF = sqlContext.createDataFrame(data, schema)

    // copy to redshift
    val RedShift = new RedShift(redshiftHost, redshiftUser, redshiftPassword, awsKey, awsSecret)
    RedShift.CopyFromDataFrame(DF, table, S3Path)
  }
}
