package com.secretsales.analytics.table

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.sql.{ResultSet, Timestamp}

class OrderRefundTable extends Table {
  val mysqlTable = "orders_refund"
  val mysqlKey = "id"
  val redshiftTable = "order_refunds"
  val redshiftKey = "order_refund_id"
  val totalRecords = 10000
  val batchSize = 1000
  val partitions = totalRecords/batchSize
  val baseSelectQuery = "SELECT `id`, `VendorTxCode`, `amount` , `reason`, `orderId`, `created` FROM orders_refund"

  def getSchema(): StructType = {
    return StructType(Array(
      StructField("order_refund_id",LongType,true),
      StructField("checkout_id",StringType,true),
      StructField("order_id",LongType,true),
      StructField("amount",FloatType,true),
      StructField("reason",StringType,true),
      StructField("created",StringType,true),
      StructField("updated",StringType,true)
    ))
  }

  def getMappedRow(r: ResultSet): Row = {
    return Row(
      r.getLong("id"),
      r.getString("VendorTxCode"),
      r.getLong("orderId"),
      r.getFloat("amount"),
      r.getString("reason"),
      r.getString("created"),
      "" //updated_at missing in mysql
    )
  }

  def newRowQuery(): String = {
    return baseSelectQuery +" WHERE id >= ? AND id <= ?"
  }

  def recentlyUpdatedRowQuery(lastUpdated: Timestamp): String = {
    return ""
  }
}
