package com.zhiyou.bd14

import org.apache.spark.sql.SparkSession

case class Order(orderId: String, orderDate: String, customerId: String, status: String)

object HiveConnectTest {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("connect hive")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._
  import spark.sql

  def testConnectionHive() = {
    //    val tableFromHive = sql("create database test01")
    val tableFromHive = sql("show databases")
    tableFromHive.show()
  }

  //保存数据到hive中
  def writeToHive() = {
    val rdd = spark.sparkContext.textFile("/user/orderdata/orders")
    val ds = rdd.map(x => {
      val info = x.split("\\|")
      Order(info(0), info(1), info(2), info(3))
    }).toDS()
    ds.createOrReplaceTempView("orders")
    val sqlResult = sql(
      """
        |create table bigdata14.customer_order_num as
        |select customerId
        |       , count(1) as order_num
        |from orders
        |group by customerId
      """.stripMargin
    )
    sqlResult.printSchema()
  }


  def main(args: Array[String]): Unit = {
    testConnectionHive()
//    writeToHive()
  }
}
