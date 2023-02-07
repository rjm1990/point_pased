

package com.seism.test

import com.seism.test.PgSqlUtil.insertOrUpdateToPgsql
import org.apache.spark.sql.SparkSession


object Testpg {

  /**
  * 测试用例
  * 批量保存数据
   * , 存在则更新 不存在 则插入
  * INSERT INTO test_001 VALUES(?, ?, ?)
  * ON conflict(ID) DO
  * UPDATE SET id =?
   * , NAME = ?
   * , age = ?;
   *
  * test
   * id	varchar	20	0	False	True			pg_catalog	default	0				0		0	0	0	0	False	0	False	0	0		False
   * name	varchar	32	0	True	False			pg_catalog	default	0				0		0	0	0	0	False	0	False	0	0		False
   *
   * test001
   * id	varchar	32	0	False	True			pg_catalog	default	0				0		0	0	0	0	False	0	False	0	0		False
   * name	varchar	255	0	True	False			pg_catalog	default	0				0		0	0	0	0	False	0	False	0	0		False
   *
   * @author linzhy
  */


    def main(args: Array[String]): Unit = {

      val spark = SparkSession.builder()
        .appName(this.getClass.getSimpleName)
        .master("local[2]")
        .config("spark.debug.maxToStringFields", "100")
        .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
//      var config = ConfigFactory.load()

      val ods_url = "jdbc:postgresql://10.13.155.192:5432/fxfzaqbz"

      val ods_user = "postgres"

      val ods_password = "123456"

      val test_001 = spark.read.format("jdbc")
        .option("url", ods_url)
        .option("dbtable", "test001")
        .option("user", ods_user)
        .option("password", ods_password)
        .load()

      test_001.createOrReplaceTempView("test_001")

      val sql =
        """
          |SELECT id,name FROM test_001
          |""".stripMargin
      val dataFrame = spark.sql(sql)

      //批量保存数据,存在则更新 不存在 则插入
      insertOrUpdateToPgsql(dataFrame, spark.sparkContext, "test", "id")

      spark.stop();

    }





}
