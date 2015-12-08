package com.secretsales.analytics

class Config() {
  // val mysqlUrl = sys.env("MYSQL_HOST")
  // val mysqlUser = sys.env("MYSQL_USER")
  // val mysqlPassword = sys.env("MYSQL_PASS")
  // val redshiftUrl = sys.env("REDSHIFT_HOST")
  // val redshiftUser = sys.env("REDSHIFT_USER")
  // val redshiftPassword = sys.env("REDSHIFT_PASS")
  // val awsKey = sys.env("AWS_ACCESS_KEY_ID")
  // val awsSecret = sys.env("AWS_SECRET_ACCESS_KEY")

  // these needs to be functions, otherwise spark cant serialize it
  def mysqlHost() : String = sys.env("MYSQL_HOST")
}
