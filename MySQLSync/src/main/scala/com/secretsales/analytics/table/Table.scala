package com.secretsales.analytics.table
/**
 * Provides classes for mapping MySQL tables in to RedShift
 */

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.sql.ResultSet

/*
 * Table interface
 */
trait Table extends java.io.Serializable {
  val mysqlTable : String
  val mysqlKey : String
  val redshiftTable : String
  val redshiftKey : String
  val batchSize: Int
  val partitions: Int

  /** Returns table schema */
  def getSchema() : StructType

  /** Maps a ResultSet to a Row */
  def getMappedRow(r : ResultSet) : Row

  /** SQL for extracting data from source (i.e. SELECT * FROM table WHERE pkey > lastId) */
  def getExtractSql(lastId : Long, lastUpdated : String) : String
}
