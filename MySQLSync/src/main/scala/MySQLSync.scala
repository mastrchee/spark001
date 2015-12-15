import org.apache.spark.rdd.JdbcRDD
import java.sql.{Connection, DriverManager, ResultSet, Timestamp}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.secretsales.analytics.database.RedShift
import com.secretsales.analytics.retriever._
import com.secretsales.analytics.table._

object MySQLSync {
  def main(args: Array[String]) {
    // Environment Vars (Will throw exception if not set)
    val mysqlHost = System.getenv("MYSQL_HOST")
    val mysqlUser = System.getenv("MYSQL_USER")
    val mysqlPassword = System.getenv("MYSQL_PASS")
    val redshiftHost = System.getenv("REDSHIFT_HOST")
    val redshiftUser = System.getenv("REDSHIFT_USER")
    val redshiftPassword = System.getenv("REDSHIFT_PASS")
    val awsKey = System.getenv("AWS_ACCESS_KEY_ID")
    val awsSecret = System.getenv("AWS_SECRET_ACCESS_KEY")

    // setup SparkContext
    val sparkConf = new SparkConf()
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    val applicationId = sparkContext.applicationId.replaceAll(" ", "").replaceAll("[^a-zA-Z0-9]", "_")
    import sqlContext.implicits._

    // tables to sync
    val tables = Array(
      new CollectionTable(),
      new UserTable(),
      new OrderTable(),
      new OrderDetailTable(),
      new OrderRefundTable()
    )

    // mapper function
    def syncTable (table: Table) {
      var s3Path = "secretsales-analytics/RedShift/Load/"+table.mysqlTable+"/"+applicationId
      var RedShift = new RedShift(redshiftHost, redshiftUser, redshiftPassword, awsKey, awsSecret)

      // get the latest rows in redshift
      val redshiftLatestRowRetriever = new LatestRowRetriever(sqlContext, redshiftHost, redshiftUser, redshiftPassword, "redshift")
      val latestRedshiftRow : LatestRow = redshiftLatestRowRetriever.getLatest(table.redshiftTable, table.redshiftKey, table.redshiftUpdated)

      // get the latest rows in mysql
      val mysqlLatestRowRetriever = new LatestRowRetriever(sqlContext, mysqlHost, mysqlUser, mysqlPassword, "mysql")
      val latestMysqlRow : LatestRow = mysqlLatestRowRetriever.getLatest(table.mysqlTable, table.mysqlKey, table.mysqlUpdated)

      val batchLimit = 1000 //per query => per csv

      // get recently updated rows
      if (table.mysqlUpdated != null && latestRedshiftRow.lastUpdated != latestMysqlRow.lastUpdated) {
        val recentlyUpdated = new JdbcRDD(sparkContext,
          () => DriverManager.getConnection(mysqlHost, mysqlUser, mysqlPassword),
          table.recentlyUpdatedRowQuery(latestRedshiftRow.lastId, latestRedshiftRow.lastUpdated) + " LIMIT " + batchLimit,
          1, 1, 1, r => table.getMappedRow(r)
        )

        val recentlyUpdatedDataFrame = sqlContext.createDataFrame(recentlyUpdated, table.getSchema())
        RedShift.CopyFromDataFrame(recentlyUpdatedDataFrame, table.redshiftTable, s3Path + "/updated", table.redshiftKey, applicationId + "_staging_updated_")
      }

      if (latestRedshiftRow.lastId != latestMysqlRow.lastId) {
        val records = (latestMysqlRow.lastId - latestRedshiftRow.lastId)
        var maxRecords = 500000

        if (records < maxRecords) {
          maxRecords = ((records - (records % batchLimit))).toInt + batchLimit
        }

        val partitions = maxRecords/batchLimit
        val lowerBound = latestRedshiftRow.lastId + 1
        val upperBound = latestRedshiftRow.lastId + maxRecords

        // get new rows
        val newRows = new JdbcRDD(sparkContext,
          () => DriverManager.getConnection(mysqlHost, mysqlUser, mysqlPassword),
          table.newRowQuery(),
           lowerBound, upperBound, partitions, r => table.getMappedRow(r)
        )

        // convert to DataFrame
        val newRowsDataFrame = sqlContext.createDataFrame(newRows, table.getSchema())
        RedShift.CopyFromDataFrame(newRowsDataFrame, table.redshiftTable, s3Path + "/new", table.redshiftKey, applicationId + "_staging_new_")
      }
    }

    tables.map(syncTable)
  }
}
