package com.secretsales.analytics.table

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.sql.{ResultSet, Timestamp}

class OrderTable extends Table {
  val mysqlTable = "orders"
  val mysqlKey = "order_id"
  val mysqlUpdated = "updated_at"
  val redshiftTable = "orders"
  val redshiftKey = "order_id"
  val redshiftUpdated = "updated"
  val baseSelectQuery = "SELECT `order_id`, `discount`, `user_id`, `total_price`, `discountcode`, `delivery_method`, `delivery_price`, left(`VendorTxCode`, 2) as 'payment_method', `VendorTxCode`, `order_progress_id`, `added`, `updated_at`, `vat`, `vat_value` FROM orders"

  def getSchema() : StructType ={
    return StructType(Array(
      StructField("order_id",LongType,true),
      StructField("user_id",LongType,true),
      StructField("checkout_id",StringType,true),
      StructField("order_total",FloatType,true),
      StructField("discount",FloatType,true),
      StructField("discountcode",StringType,true),
      StructField("delivery_price",FloatType,true),
      StructField("order_progress_id",IntegerType,true),
      StructField("payment_method",StringType,true),
      StructField("vat",FloatType,true),
      StructField("vat_value",FloatType,true),
      StructField("created",StringType,true),
      StructField("updated",StringType,true)
    ))
  }

  def getMappedRow(r : ResultSet) : Row = {
    return Row(
      r.getLong("order_id"),
      r.getLong("user_id"),
      if (r.getString("VendorTxCode") == null) "" else r.getString("VendorTxCode"),
      r.getFloat("total_price"),
      r.getFloat("discount"),
      r.getString("discountcode"),
      r.getFloat("delivery_price"),
      r.getInt("order_progress_id"),
      r.getString("payment_method") match {
        case "AM" => "Amazon"
        case "PP" => "PayPal"
        case "OO" => "Ogone"
        case "AY" => "Adyen"
        case _    => "Other"
      },
      r.getFloat("vat"),
      r.getFloat("vat_value"),
      if (r.getString("added") == null) "" else r.getString("added"),
      if (r.getString("updated_at") == null) "" else r.getString("updated_at")
    )
  }
}
