package com.seism.util

import org.apache.spark.sql.execution.datasources.jdbc2.JDBCSaveMode
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
 * https://blog.csdn.net/weixin_42186387/article/details/112937594
 * mysql upsert支持  pg不支持
 */

object Test{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("jdbc2")
      .master("local[*]")
      .getOrCreate()

    val readSchmeas = StructType(
      Array(
        StructField("userid", StringType, nullable = false),
        StructField("time", StringType, nullable = false),
        StructField("indicator", LongType, nullable = false)
      )
    )

    val rdd = spark.sparkContext.parallelize(
      Array(
        Row.fromSeq(Seq("lake", "2019-02-01", 10L)),
        Row.fromSeq(Seq("admin", "2019-02-01", 10L)),
        Row.fromSeq(Seq("admin", "2019-02-01", 11L)),
        Row.fromSeq(Seq("admin", "2019-02-01", 12L))
      )
    )

    spark.createDataFrame(rdd, readSchmeas).createTempView("log")

    spark.sql("select time,count(userid) as pv,count(distinct(userid)) as uv from log group by time")
      .write
      .format("org.apache.spark.sql.execution.datasources.jdbc2")
      .options(
//        Map(
//          "savemode" -> JDBCSaveMode.Update.toString,
//          "driver" -> "org.postgresql.Driver",
//          "url" -> "jdbc:postgresql://10.13.155.192:5432/fxfzaqbz",
//          "user" -> "postgres",
//          "password" -> "123456",
//          "dbtable" -> "test",
//          "useSSL" -> "false",
//          "duplicateIncs" -> "pv,uv",
//          "showSql" -> "true"
//        )
        Map(
            "savemode" -> JDBCSaveMode.Update.toString,
            "driver" -> "com.mysql.jdbc.Driver",
            "url" -> "jdbc:mysql://81.70.77.157:33306/test",
            "user" -> "root",
            "password" -> "xiaoqi",
            "dbtable" -> "test",
            "useSSL" -> "false",
            "duplicateIncs" -> "pv,uv",
            "showSql" -> "true"
          )
      ).save()
  }
}
