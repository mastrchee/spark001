package com.secretsales.analytics.retriever.redshift

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.sql.Timestamp

/**
 * class for wrapping latest row result with correct types
 */
case class LatestRow(lastId: Long, lastUpdated: Timestamp)

/**
 * Retrieves the latest row information from a redshift table
 * Use for batch syncing from another source to redshift
 */
class LatestRowRetriever (
  sqlContext :SQLContext,
  host: String,
  user: String,
  password: String,
  driver: String = "com.amazon.redshift.jdbc41.Driver"
) {
  def getLatest(table: String, key: String): LatestRow = {
    val tmp = sqlContext.read.format("jdbc").options(Map(
        ("driver", driver),
        ("url", host),
        ("user", user),
        ("password", password),
        ("dbtable", "(select max("+key+") as last_id, max(updated) as last_updated, getdate() as time_now from "+table+") tmp")
      )).load()

    val row = tmp.select("last_id", "last_updated", "time_now").first()

    return LatestRow(
      row.getLong(0),
      if (row.getTimestamp(1) == null) row.getTimestamp(2) else row.getTimestamp(1)
    )
  }
}
