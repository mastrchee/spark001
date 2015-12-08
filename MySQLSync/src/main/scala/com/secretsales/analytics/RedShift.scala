package com.secretsales.analytics

import java.sql.{Connection, DriverManager}
import org.apache.spark.sql.DataFrame

class RedShift(
  val host: String,
  val user: String,
  val password: String,
  val AWSKey: String,
  val AWSSecret: String
) {

  def getConnection() : Connection = {
    return DriverManager.getConnection(host,user,password)
  }

  def CopyFromS3(Table: String, S3Path: String) {
    val redshift = getConnection()
    var sql = "COPY "+Table+" FROM 's3://"+S3Path+"' credentials 'aws_access_key_id="+AWSKey+";aws_secret_access_key="+AWSSecret+"' csv"
    var insert = redshift.prepareStatement(sql)
    insert.executeUpdate()
  }

  def CopyFromDataFrame(DF: DataFrame, Table: String, S3Path: String) {
    DF.write
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .save("s3n://" + S3Path)

    CopyFromS3(Table, S3Path+"/part-")
  }
}
