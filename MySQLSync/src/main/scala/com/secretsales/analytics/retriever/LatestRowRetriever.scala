package com.secretsales.analytics.retriever

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.sql.Timestamp

/**
 * class for wrapping latest row result with correct types
 */
case class LatestRow(lastId: Long, lastUpdated: Timestamp)

/**
 * Retrieves the latest row information from a given table
 * Use for batch syncing from another source to redshift
 */
class LatestRowRetriever (
  sqlContext :SQLContext,
  host: String,
  user: String,
  password: String,
  driver: String
) {

  val drivers: Map[String, String] = Map(
    ("mysql", "com.mysql.jdbc.Driver"),
    ("redshift", "com.amazon.redshift.jdbc41.Driver")
  )

  val timeFunction: Map[String, String] = Map(
    ("mysql", "now()"),
    ("redshift", "getdate()")
  )

  def getLatest(table: String, key: String, updated: String = null): LatestRow = {
    var sql = "(select max("+key+") as last_id, "+timeFunction(driver)+" as last_updated from "+table+") tmp"
    if (updated != null) {
      sql = "(select max("+key+") as last_id, COALESCE(max("+updated+"), "+timeFunction(driver)+") as last_updated from "+table+") tmp"
    }

    val tmp = sqlContext.read.format("jdbc").options(Map(
        ("driver", drivers(driver)),
        ("url", host),
        ("user", user),
        ("password", password),
        ("dbtable", sql)
      )).load()

    val row = tmp.select("last_id", "last_updated").first()

    return LatestRow(
      if (row.get(0) == null) 0 else row.get(0).toString.toLong,
      row.getTimestamp(1)
    )
  }
}
