package com.seism

import com.seism.util.AreaBoundaryUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}

//定义实体
case class Data1(id:Int,name:String)
case class Data2(id:Int,name:String)

object PointParse {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder()
      .appName("PointParse")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()
    import session.implicits._
    //获取数据
    val data1: Dataset[Data1] = session.sql("select * from aaa").as[Data1]
    val data2: Dataset[Data2] = session.sql("select * from bbb").as[Data2]
    //广播数据

    val rows: Array[Data2] = data2.collect()
    val d2: Broadcast[Array[Data2]] = session.sparkContext.broadcast(rows)


    val d1new: Dataset[Data1] = data1.map(line => {
      val str: String = line.name
      val value1: Array[Data2] = d2.value
      //TODO 这里判断点是否在面中,将信息补全
//      val bool: Boolean = AreaBoundaryUtil.isPointInPolygon()

      //返回数据
      Data1(1,line.toString())
    })

//    val frame: DataFrame = d1new.toDF()
    d1new.createTempView("aaa")

    //TODO 这里将补充完的数据写入到新表中
    session.sql("insert into table aaa select ***")

    session.stop()
  }
}
