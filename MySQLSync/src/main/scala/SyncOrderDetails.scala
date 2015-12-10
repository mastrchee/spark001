import org.apache.spark.rdd.JdbcRDD
import java.sql.{Connection, DriverManager, ResultSet, Timestamp}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.secretsales.analytics.database.RedShift

object SyncOrderDetails {
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

    val table = "order_details"
    val tableUniqueKey = "order_detail_id"
    val tableLastUpdatedKey = "last_updated"
    val S3Path = "secretsales-analytics/RedShift/Load/"+table+"/"+applicationId

    val tmp = sqlContext.read.format("jdbc").options(Map(
        ("url", redshiftHost),
        ("user", redshiftUser),
        ("password", redshiftPassword),
        ("dbtable","(select max("+tableUniqueKey+") as last_id, max(updated) as last_updated, getdate() as time_now from "+table+") tmp"),
        ("driver", "com.amazon.redshift.jdbc41.Driver")
      )).load()

      val row = tmp.select("last_id", "last_updated", "time_now").first()
      val tableLastId = row.getLong(0)
      val tableLastUpdated = if (row.getTimestamp(1) == null) row.getTimestamp(2).toString else row.getTimestamp(1).toString

    // get data
    val data = new JdbcRDD(sparkContext,
      () => DriverManager.getConnection(mysqlHost, mysqlUser, mysqlPassword),
      "SELECT od.order_detail_id, o.order_id, pc.sku, IFNULL(p.collection_id, 0) as 'collection_id', o.total_price, od.price, IFNULL(pc.cost_price, 0) as 'cost_price', o.discount, o.vat, o.vat_value, o.added FROM order_details od INNER JOIN orders o ON o.order_id = od.order_id INNER JOIN products p ON p.id = od.product_id INNER JOIN product_options po ON (od.product_id =  po.product_id AND od.option_id = po.id) INNER JOIN product_collection pc ON pc.collection_id = p.collection_id AND pc.sku = po.sku WHERE od.order_detail_id > "+tableLastId+" LIMIT ?, ?",
      0, 10000, 100, r => Row(
        r.getInt("order_detail_id"),
        r.getInt("order_id"),
        r.getString("sku"),
        r.getInt("collection_id"),
        r.getFloat("cost_price"),
        r.getFloat("price"),
        if (r.getFloat("discount") > 0) (r.getFloat("price")/r.getFloat("total_price")).toFloat*r.getFloat("discount") else 0.0f, // disount per product (ternary check because of s**tty data)
        if (r.getFloat("vat") > 0) (r.getFloat("price")/(100.0f+r.getFloat("vat"))) * r.getFloat("vat") else 0.0f, // vat_value per product (ternary check because of s**tty data)
        if (r.getString("added") == null) "" else r.getString("added"),
        ""
      )
    )

    // create schema map
    val schema = StructType(Array(
      StructField("order_detail_id",IntegerType,true),
      StructField("order_id",IntegerType,true),
      StructField("sku",StringType,true),
      StructField("collection_id",IntegerType,true),
      StructField("cost_price",FloatType,true),
      StructField("price",FloatType,true),
      StructField("discount",FloatType,true),
      StructField("vat_value",FloatType,true),
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
