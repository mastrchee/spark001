import org.apache.spark.rdd.JdbcRDD
import java.sql.{Connection, DriverManager, ResultSet, Timestamp}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.secretsales.analytics.RedShift

object SyncUsers {
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

    val table = "users"
    val tableKey = "user_id"
    val S3Path = "secretsales-analytics/RedShift/Load/"+table+"/"+(System.currentTimeMillis / 1000)

    val tmp = sqlContext.read.format("jdbc").options(Map(
        ("url", redshiftHost),
        ("user", redshiftUser),
        ("password", redshiftPassword),
        ("dbtable","(select max("+tableKey+") as max_id from "+table+") tmp"),
        ("driver", "com.amazon.redshift.jdbc41.Driver")
      )).load()

    val maxId=tmp.select("max_id").first().getLong(0)

    // get data
    val data = new JdbcRDD(sparkContext,
      () => DriverManager.getConnection(mysqlHost, mysqlUser, mysqlPassword),
      "SELECT user_id, gender, partnership, last_login, created, last_updated FROM users WHERE user_id > "+maxId+" LIMIT ?, ?",
      0, 10000, 100, r => Row(
        r.getInt("user_id"),
        r.getString("gender") match {
          case "m" => "male"
          case "f" => "female"
          case _  => "unknown"
        },
        r.getString("partnership"),
        if (r.getString("last_login") == null) "" else r.getString("last_login"),
        if (r.getString("created") == null) "" else r.getString("created"),
        if (r.getString("last_updated") == null) "" else r.getString("last_updated")
      )
    )

    // create schema map
    val schema = StructType(Array(
      StructField("user_id",IntegerType,true),
      StructField("gender",StringType,true),
      StructField("partnership",StringType,true),
      StructField("last_login",StringType,true),
      StructField("created",StringType,true),
      StructField("last_updated",StringType,true)
    ))

    // convert to DataFrame
    val DF = sqlContext.createDataFrame(data, schema)

    // copy to redshift
    val RedShift = new RedShift(redshiftHost, redshiftUser, redshiftPassword, awsKey, awsSecret)
    RedShift.CopyFromDataFrame(DF, table, S3Path)
  }
}
