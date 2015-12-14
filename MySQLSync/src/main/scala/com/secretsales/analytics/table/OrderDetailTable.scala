package com.secretsales.analytics.table

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.sql.{ResultSet, Timestamp}

class OrderDetailTable extends Table {
  val mysqlTable = "order_details"
  val mysqlKey = "order_detail_id"
  val redshiftTable = "order_details"
  val redshiftKey = "order_detail_id"
  val totalRecords = 100000
  val batchSize = 1000
  val partitions = totalRecords/batchSize
  val baseSelectQuery = "SELECT od.order_detail_id, o.order_id, pc.sku, IFNULL(p.collection_id, 0) as 'collection_id', o.total_price, od.price, IFNULL(pc.cost_price, 0) as 'cost_price', o.discount, o.vat, o.vat_value, o.added, od.updated_at FROM order_details od INNER JOIN orders o ON o.order_id = od.order_id INNER JOIN products p ON p.id = od.product_id INNER JOIN product_options po ON (od.product_id =  po.product_id AND od.option_id = po.id) INNER JOIN product_collection pc ON pc.collection_id = p.collection_id AND pc.sku = po.sku"

  def getSchema() : StructType ={
    return StructType(Array(
      StructField("order_detail_id",LongType,true),
      StructField("order_id",LongType,true),
      StructField("sku",StringType,true),
      StructField("collection_id",LongType,true),
      StructField("cost_price",FloatType,true),
      StructField("price",FloatType,true),
      StructField("discount",FloatType,true),
      StructField("vat_value",FloatType,true),
      StructField("created",StringType,true),
      StructField("updated",StringType,true)
    ))
  }

  def getMappedRow(r : ResultSet) : Row = {
    return Row(
      r.getLong("order_detail_id"),
      r.getLong("order_id"),
      r.getString("sku"),
      r.getLong("collection_id"),
      r.getFloat("cost_price"),
      r.getFloat("price"),
      if (r.getFloat("discount") > 0) (r.getFloat("price")/r.getFloat("total_price")).toFloat*r.getFloat("discount") else 0.0f, // disount per product (ternary check because of s**tty data)
      if (r.getFloat("vat") > 0 && r.getFloat("vat_value") > 0) (r.getFloat("price")/(100.0f+r.getFloat("vat"))) * r.getFloat("vat") else 0.0f, // vat_value per product (ternary check because of s**tty data)
      if (r.getString("added") == null) "" else r.getString("added"),
      if (r.getString("updated_at") == null) "" else r.getString("updated_at")
    )
  }

  def newRowQuery() : String = {
    return baseSelectQuery + " WHERE od.order_detail_id >= ? AND od.order_detail_id <= ?"
  }

  def recentlyUpdatedRowQuery(latestId : Long, lastUpdated: Timestamp): String = {
    return baseSelectQuery + " WHERE ? = ? AND od.order_detail_id <= "+latestId+" AND od.updated_at > '"+lastUpdated.toString+"' LIMIT "+batchSize
  }
}
