package com.secretsales.analytics.table

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.sql.{ResultSet, Timestamp}

class UserTable extends Table {
  val mysqlTable = "users"
  val mysqlKey = "user_id"
  val mysqlUpdated = "last_updated"
  val redshiftTable = "users"
  val redshiftKey = "user_id"
  val redshiftUpdated = "updated"
  val baseSelectQuery = "SELECT user_id, gender, partnership, last_login, created, last_updated FROM users"

  def getSchema(): StructType = {
    return StructType(Array(
      StructField("user_id",LongType,true),
      StructField("gender",StringType,true),
      StructField("partnership",StringType,true),
      StructField("last_login",StringType,true),
      StructField("created",StringType,true),
      StructField("last_updated",StringType,true)
    ))
  }

  def getMappedRow(r: ResultSet): Row = {
    return Row(
      r.getLong("user_id"),
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
  }
}
