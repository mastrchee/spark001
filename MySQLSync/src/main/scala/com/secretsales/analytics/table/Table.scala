package com.secretsales.analytics.table
/**
 * Provides classes for mapping MySQL tables in to RedShift
 */

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.sql.{ResultSet, Timestamp}

/*
 * Table interface
 */
trait Table extends java.io.Serializable {
  val mysqlTable : String
  val mysqlKey : String
  val redshiftTable : String
  val redshiftKey : String
  val totalRecords: Long
  val batchSize: Long
  val partitions: Long

  /** Returns table schema */
  def getSchema() : StructType

  /** Maps a ResultSet to a Row */
  def getMappedRow(r : ResultSet) : Row

  def newRowQuery() : String

  def recentlyUpdatedRowQuery(lastUpdated : Timestamp) : String
}
