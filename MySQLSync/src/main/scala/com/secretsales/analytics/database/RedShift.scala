package com.secretsales.analytics.database

import java.sql.{Connection, DriverManager}
import org.apache.spark.sql.DataFrame

class RedShift(
  val host: String,
  val user: String,
  val password: String,
  val awsKey: String,
  val awsSecret: String,
  val staginTablePrefix: String = "staging_"
) {

  private def getConnection() : Connection = {
    return DriverManager.getConnection(host,user,password)
  }

  def CopyFromS3(table:String, s3Path: String) {
    val redshift = getConnection()
    val sql = "COPY "+table+" FROM 's3://"+s3Path+"' credentials 'aws_access_key_id="+awsKey+";aws_secret_access_key="+awsSecret+"' csv"
    val insert = redshift.prepareStatement(sql)
    insert.executeUpdate()
  }

  def CopyFromS3Unique(table:String, s3Path: String, uniqueKey: String) {
    val redshift = getConnection()
    val stagingTable = staginTablePrefix + table
    val sql = "BEGIN TRANSACTION; "+
    "CREATE TABLE "+stagingTable+" LIKE "+table+"; "+
    "COPY "+stagingTable+" FROM 's3://"+s3Path+"' credentials 'aws_access_key_id="+awsKey+";aws_secret_access_key="+awsSecret+"' csv; "+
    "DELETE FROM "+table+" USING "+stagingTable+" WHERE "+table+"."+uniqueKey+" = "+stagingTable+"."+uniqueKey+"; "+
    "INSERT INTO "+table+" SELECT * FROM "+stagingTable+" "+
    "END TRANSACTION; "+
    "DROP TABLE "+stagingTable+";"
    val insert = redshift.prepareStatement(sql)
    insert.executeUpdate()
  }

  def CopyFromDataFrame(source: DataFrame, table:String, s3Path: String, uniqueKey: String = null) {
    source.write
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("charset", "UTF-8")
      .save("s3n://" + s3Path)

    if (uniqueKey != null) {
      CopyFromS3(table, s3Path+"/part-")
    } else {
      CopyFromS3Unique(table, s3Path+"/part-", uniqueKey)
    }
  }
}
