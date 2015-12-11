package com.secretsales.analytics.table

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.sql.ResultSet

class CollectionTable extends Table {
  val mysqlTable = "collections"
  val mysqlKey = "id"
  val redshiftTable = "collections"
  val redshiftKey = "collection_id"
  val batchSize = 100
  val partitions = 1

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

  def getExtractSql(lastId : Long, lastUpdated: String) : String = {
    return "SELECT `id`, `name`, `created_at`, `updated_at` FROM collections WHERE id > "+lastId+" OR updated_at > '"+lastUpdated+"'"
  }
}
