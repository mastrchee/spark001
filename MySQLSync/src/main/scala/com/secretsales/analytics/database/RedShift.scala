package com.secretsales.analytics.database

import java.sql.{Connection, DriverManager}
import org.apache.spark.sql.DataFrame

/**
 * RedShift Database provider
 */
class RedShift (
  val host: String,
  val user: String,
  val password: String,
  val awsKey: String,
  val awsSecret: String,
  val staginTablePrefix: String = "staging_"
) {
  /** Returns DB Connection */
  private def getConnection() : Connection = {
    return DriverManager.getConnection(host,user,password)
  }

  /** Initiates COPY (csv) command to RedShift using S3 */
  def CopyFromS3(table : String, s3Path: String) {
    val redshift = getConnection()
    val sql = "COPY "+table+" FROM 's3://"+s3Path+"' credentials 'aws_access_key_id="+awsKey+";aws_secret_access_key="+awsSecret+"' csv"
    val insert = redshift.prepareStatement(sql)
    insert.executeUpdate()
  }

  /** Initiates COPY (csv) command to RedShift using S3 and maintains unique keys */
  def CopyFromS3Unique(table : String, s3Path : String, uniqueKey : String) {
    val redshift = getConnection()
    val stagingTable = staginTablePrefix + table
    var sql = "CREATE TABLE "+stagingTable+" (LIKE "+table+");"
    redshift.prepareStatement(sql).executeUpdate();
    sql = "BEGIN TRANSACTION; "
    redshift.prepareStatement(sql).executeUpdate();
    sql = "COPY "+stagingTable+" FROM 's3://"+s3Path+"' credentials 'aws_access_key_id="+awsKey+";aws_secret_access_key="+awsSecret+"' csv; "
    redshift.prepareStatement(sql).executeUpdate();
    sql = "DELETE FROM "+table+" USING "+stagingTable+" WHERE "+table+"."+uniqueKey+" = "+stagingTable+"."+uniqueKey+"; "
    redshift.prepareStatement(sql).executeUpdate();
    sql = "INSERT INTO "+table+" SELECT * FROM "+stagingTable+"; "
    redshift.prepareStatement(sql).executeUpdate();
    sql = "END TRANSACTION; "
    redshift.prepareStatement(sql).executeUpdate();
    sql = "DROP TABLE "+stagingTable+"; "
    redshift.prepareStatement(sql).executeUpdate();
  }

  /** Creates a CSV file in S3 and attempts to copy it to RedShift */
  def CopyFromDataFrame(source: DataFrame, table : String, s3Path : String, uniqueKey : String = null) {
    source.write
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("charset", "UTF-8")
      .save("s3n://" + s3Path)

    if (uniqueKey != null) {
      CopyFromS3Unique(table, s3Path+"/part-", uniqueKey)
    } else {
      CopyFromS3(table, s3Path+"/part-")
    }
  }
}
