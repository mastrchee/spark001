import org.apache.spark.rdd.JdbcRDD
import java.sql.{Connection, DriverManager, ResultSet, Timestamp}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.secretsales.analytics.database.RedShift

object SyncCollections {
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

    val table = "collections"
    val tableUniqueKey = "collection_id"
    val tableLastUpdatedKey = "updated_at"
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
      "SELECT `id`, `name`, `created_at`, `updated_at` FROM collections WHERE id > "+tableLastId+" OR updated_at > '"+tableLastUpdated+"' LIMIT ?, ?",
      0, 10000, 100, r => Row(
        r.getInt("id"),
        r.getString("name"),
        if (r.getString("created_at") == null) "" else r.getString("created_at"),
        if (r.getString("updated_at") == null) "" else r.getString("updated_at")
      )
    )

    // create schema map
    val schema = StructType(Array(
      StructField("collection_id",IntegerType,true),
      StructField("collection_name",StringType,true),
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
