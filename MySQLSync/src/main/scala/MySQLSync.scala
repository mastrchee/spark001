import org.apache.spark.rdd.JdbcRDD
import java.sql.{Connection, DriverManager, ResultSet, Timestamp}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.secretsales.analytics.database.RedShift
import com.secretsales.analytics.retriever.redshift._
import com.secretsales.analytics.table._

object MySQLSync {
  def main(args: Array[String]) {
    val mysqlHost = System.getenv("MYSQL_HOST")
    val mysqlUser = System.getenv("MYSQL_USER")
    val mysqlPassword = System.getenv("MYSQL_PASS")
    val redshiftHost = System.getenv("REDSHIFT_HOST")
    val redshiftUser = System.getenv("REDSHIFT_USER")
    val redshiftPassword = System.getenv("REDSHIFT_PASS")
    val awsKey = System.getenv("AWS_ACCESS_KEY_ID")
    val awsSecret = System.getenv("AWS_SECRET_ACCESS_KEY")
    var batchSize = 1000
    var partitionSize = 10

    val tables = Array(
      new UserTable(),
      new OrderTable(),
      new OrderDetailTable(),
      new CollectionTable()
    )

    for (table <- tables) {
      val sparkConf = new SparkConf().setAppName("sync_"+table.redshiftTable)
      val sparkContext = new SparkContext(sparkConf)
      val sqlContext = new SQLContext(sparkContext)
      val applicationId = sparkContext.applicationId
      import sqlContext.implicits._

      val redshiftLatestRowRetriever = new LatestRowRetriever(sqlContext, redshiftHost, redshiftUser, redshiftPassword)
      val latestRedshiftRow : LatestRow = redshiftLatestRowRetriever.getLatest(table.redshiftTable, table.redshiftKey)

      var sql = table.getExtractSql(latestRedshiftRow.lastId, latestRedshiftRow.lastUpdated.toString)

      // get data
      val data = new JdbcRDD(sparkContext,
        () => DriverManager.getConnection(mysqlHost, mysqlUser, mysqlPassword),
        sql+" LIMIT ?, ?",
        0, batchSize, partitionSize, r => table.getMap(r)
      )

      // convert to DataFrame
      val dataDF = sqlContext.createDataFrame(data, table.getSchema())

      // copy to redshift
      var s3Path = "secretsales-analytics/RedShift/Load/"+table.mysqlTable+"/"+applicationId
      var RedShift = new RedShift(redshiftHost, redshiftUser, redshiftPassword, awsKey, awsSecret, applicationId + "_staging_")

      RedShift.CopyFromDataFrame(dataDF, table.redshiftTable, s3Path, table.redshiftKey)
      sparkContext.stop()
    }
  }
}
