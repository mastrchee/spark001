package com.secretsales.analytics

import java.sql.{Connection, DriverManager}

class RedShift(
  val host: String,
  val user: String,
  val password: String,
  val AWSKey: String,
  val AWSSecret: String
) {

  def getConnection() : java.sql.Connection = {
    return DriverManager.getConnection(host,user,password)
  }

  def CopyCSVFromS3(Table: String, S3Path: String) {
    val redshift = getConnection()
    var sql = "COPY "+Table+" FROM 's3://"+S3Path+"' credentials 'aws_access_key_id="+AWSKey+";aws_secret_access_key="+AWSSecret+"' csv"
    var insert = redshift.prepareStatement(sql)
    insert.executeUpdate()
  }
}
