package com.secretsales.analytics.table

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.sql.ResultSet

trait Table extends java.io.Serializable {
  val mysqlTable : String;
  val mysqlKey : String;
  val redshiftTable : String;
  val redshiftKey : String;

  def getSchema() : StructType

  def getMap(r : ResultSet) : Row

  def getExtractSql(lastId : Long, lastUpdated : String) : String
}
