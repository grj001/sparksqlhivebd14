package com.zhiyou.bd14

import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

object HBaseConnectTest {


  def readFromHBase() = {

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("readFromHBase")
    val sc = new SparkContext(sparkConf)

    val tableName = "bd14:orders"
    val conf = HBaseConfiguration.create()
    //设置zookeeper集群的值, 也可以通过将hbase-site.xml导入classpath,
    // 但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum", "master,slaver1,slaver2")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    //如果表不存在则创建表
    val admin = ConnectionFactory.createConnection(conf).getAdmin()
    if (!admin.tableExists(TableName.valueOf(tableName))) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
      admin.createTable(tableDesc)
    }

    //读取数据并转化为rdd
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat]
      , classOf[ImmutableBytesWritable]
      , classOf[Result])

    val count = hBaseRDD.count()
    println(count)

    hBaseRDD.foreach { case (_, result) => {
      //获取行键
      val key = Bytes.toString(result.getRow)

      //通过列族和列名 获取列
      val orderId = Bytes.toString(result.getValue("i".getBytes, "order_id".getBytes))
      val orderStatus = Bytes.toString(result.getValue("i".getBytes, "order_status".getBytes))
      println(s"orderId:${orderId}, orderStatus:${orderStatus}")
    }
    }

    sc.stop()
    admin.close()
  }

  def writeToHBase() = {
    val sparkConf = new SparkConf()
      .setAppName("writeToHBase")
      .setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val hBaseConf = HBaseConfiguration.create()


    val tableName = "bd14:from_spark"

    hBaseConf.set("hbase.zookeeper.quorum", "master,slaver1,slaver2")
    hBaseConf.set("hbase.zookeeper.property.clientPort", "2181")

    //指定输出格式和输出表名
    val jobConf = new JobConf(hBaseConf, this.getClass)

    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val job = Job.getInstance(hBaseConf)

    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])

    val indataRDD = sc.makeRDD(Array("1,jack,15", "2,Lily,16", "3,mike,16"))
    val rdd = indataRDD.map(_.split(',')).map { arr => {
      val put = new Put(Bytes.toBytes(arr(0)))
      put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("name"), Bytes.toBytes(arr(1)))
      put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("age"), Bytes.toBytes(arr(2).toInt))
      (new ImmutableBytesWritable, put)
    }
    }

    rdd.saveAsHadoopDataset(jobConf)


  }


  def main(args: Array[String]): Unit = {
    //    readFromHBase()
    writeToHBase()
  }


}
