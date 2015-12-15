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
  val mysqlTable: String
  val mysqlKey: String
  val mysqlUpdated: String
  val redshiftTable: String
  val redshiftKey: String
  val redshiftUpdated: String
  val baseSelectQuery: String

  /** Returns table schema */
  def getSchema(): StructType

  /** Maps a ResultSet to a Row */
  def getMappedRow(r: ResultSet): Row

  def newRowQuery(): String = {
    return baseSelectQuery +" WHERE " + mysqlKey + " >= ? AND " + mysqlKey + " <= ?"
  }

  def recentlyUpdatedRowQuery(latestId: Long, lastUpdated: Timestamp): String = {
    return baseSelectQuery +" WHERE ? = ? AND " + mysqlKey + " <= " + latestId + " AND " + mysqlUpdated + " > '" + lastUpdated.toString + "'"
  }
}
