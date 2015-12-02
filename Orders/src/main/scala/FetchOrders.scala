import org.apache.spark.rdd.JdbcRDD
import java.sql.{Connection, DriverManager, ResultSet, Timestamp}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql

object FetchOrders {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Fetch Orders")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val rds="jdbc:mysql://marketing.cemhpwpdzdl3.eu-west-1.rds.amazonaws.com:3306/secretsales?zeroDateTimeBehavior=convertToNull"
    val rdsUser = "secret_base"
    val rdsPassword = "ASDy456DFHGtyju56"

    val rs="jdbc:redshift://analytics.ctzb9bfssdrj.eu-west-1.redshift.amazonaws.com:5439/transactional"
    val rsUser = "secretsales"
    val rsPassword = "4aUeq8bJUE7Qwne8"

    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "AKIAIZTM4IA2USXMDZFA")
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "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")

    val ordersLastMin = new JdbcRDD(sc,
      () => DriverManager.getConnection(rds,rdsUser,rdsPassword),
      "select COUNT(*) as orders, SUM(total_price) as `total`, SUM(discount) as `discount`, DATE_FORMAT(DATE_SUB(CURTIME(), INTERVAL 1 MINUTE), '%Y-%m-%d %H:%i:00') as `period` from orders where added BETWEEN DATE_FORMAT(DATE_SUB(CURTIME(), INTERVAL 1 MINUTE), '%Y-%m-%d %H:%i:00') AND DATE_FORMAT(DATE_SUB(CURTIME(), INTERVAL 1 MINUTE), '%Y-%m-%d %H:%i:59') LIMIT ?, ?",
      0, 5, 2, r => Map(
          ("period", r.getString("period")),
          ("orders", r.getInt("orders")),
          ("total", r.getFloat("total")),
          ("discount", r.getFloat("discount"))
        )
    )

    val prop = new java.util.Properties
    prop.setProperty("user", rsUser)
    prop.setProperty("password", rsPassword)

    var previous = Timestamp.valueOf("1999-12-31 23:59:59")
    while (true) {
      for (order <- ordersLastMin) {
        var t = Timestamp.valueOf(order("period").toString)

        if (t != previous) {
          println(order("period")+": "+order("total") + " - " + order("discount"))

          var conn = DriverManager.getConnection(rs,rsUser,rsPassword)

          val insert = conn.prepareStatement("INSERT INTO orders VALUES(?, ?, ?, ?)")
          insert.setTimestamp(1, t)
          insert.setInt(2, order("orders").toString.toInt)
          insert.setFloat(3, order("total").toString.toFloat)
          insert.setFloat(4, order("discount").toString.toFloat)
          insert.executeUpdate()
        }
        previous = t
      }
      Thread.sleep(59000)
    }
  }

  def getOrders(args: Array[String]) {
  }
}
