package com.secretsales.analytics.table

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.sql.{ResultSet, Timestamp}

class CollectionTable extends Table {
  val mysqlTable = "collections"
  val mysqlKey = "id"
  val redshiftTable = "collections"
  val redshiftKey = "collection_id"
  val totalRecords = 1000
  val batchSize = 100
  val partitions = totalRecords/batchSize
  val baseSelectQuery = "SELECT `id`, `name`, `created_at`, `updated_at` FROM collections"

  def getSchema() : StructType ={
    return StructType(Array(
      StructField("collection_id",LongType,true),
      StructField("collection_name",StringType,true),
      StructField("created",StringType,true),
      StructField("updated",StringType,true)
    ))
  }

  def getMappedRow(r : ResultSet) : Row = {
    return Row(
      r.getLong("id"),
      r.getString("name"),
      if (r.getString("created_at") == null) "" else r.getString("created_at"),
      if (r.getString("updated_at") == null) "" else r.getString("updated_at")
    )
  }

  def newRowQuery() : String = {
    return baseSelectQuery +" WHERE id >= ? AND id <= ?"
  }

  def recentlyUpdatedRowQuery(lastUpdated: Timestamp): String = {
    return baseSelectQuery +" WHERE ? = ? AND updated_at > '"+lastUpdated.toString+"' LIMIT "+batchSize
  }
}
