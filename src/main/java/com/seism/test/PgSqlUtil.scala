package com.seism.test

import com.sun.corba.se.impl.activation.ServerMain.logError
import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.datasources.jdbc2.JdbcUtils.getCommonJDBCType
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.postgresql.jdbc2.optional.ConnectionPool

import java.sql.{Connection, PreparedStatement}
object PgSqlUtil {

  def connectionPool() = {
    val conn = new ConnectionPool()
    conn.setUser("postgres")
    conn.setPassword("123456")
    conn.setServerName("10.13.155.192")
    conn.setDatabaseName("fxfzaqbz")
    conn.setPortNumber(5432)
    conn.getConnection
  }
  /**
   * 批量插入 或更新 数据 ,该方法 借鉴Spark.write.save() 源码
   *
   * @param dataFrame
   * @param sc
   * @param table
   * @param id
   */
  def insertOrUpdateToPgsql(dataFrame: DataFrame, sc: SparkContext, table: String, id: String): Unit = {

    val tableSchema: StructType = dataFrame.schema
    val columns = tableSchema.fields.map(x => x.name).mkString(",")
    val placeholders = tableSchema.fields.map(_ => "?").mkString(",")
    val sql = s"INSERT INTO $table ($columns) VALUES ($placeholders) on conflict($id) do update set "
    val update = tableSchema.fields.map(x =>
      x.name.toString + "=?"
    ).mkString(",")

    val realsql = sql.concat(update)
    val conn = connectionPool()
    conn.setAutoCommit(false)
    val dialect = JdbcDialects.get(conn.getMetaData.getURL)
    val broad_ps = sc.broadcast(conn.prepareStatement(realsql))

    val numFields = tableSchema.fields.length * 2
    //调用spark中自带的函数 或者 捞出来,获取属性字段与字段类型
    val nullTypes = tableSchema.fields.map(f => getJdbcType(f.dataType, dialect).jdbcNullType)
    val setters = tableSchema.fields.map(f => makeSetter(conn, f.dataType))

    var rowCount = 0
    val batchSize = 2000
    val updateindex = numFields / 2
    try {
      dataFrame.rdd.foreachPartition(iterator => {
        //遍历批量提交
        val ps = broad_ps.value
        try {
          while (iterator.hasNext) {
            val row = iterator.next()
            var i = 0
            while (i < numFields) {
              i < updateindex match {
                case true => {
                  if (row.isNullAt(i)) {
                    ps.setNull(i + 1, nullTypes(i))
                  } else {
                    setters(i).apply(ps, row, i, 0)
                  }
                }
                case false => {
                  if (row.isNullAt(i - updateindex)) {
                    ps.setNull(i + 1, nullTypes(i - updateindex))
                  } else {
                    setters(i - updateindex).apply(ps, row, i, updateindex)
                  }
                }
              }
              i = i + 1
            }
            ps.addBatch()
            rowCount += 1
            if (rowCount % batchSize == 0) {
              ps.executeBatch()
              rowCount = 0
            }
          }
          if (rowCount > 0) {
            ps.executeBatch()
          }
        } finally {
          ps.close()
        }
      })
      conn.commit()
    } catch {
      case e: Exception =>
        logError("Error in execution of insert. " + e.getMessage)
        conn.rollback()
    } finally {
      conn.close()
    }
  }

  private def getJdbcType(dt: DataType, dialect: JdbcDialect): JdbcType = {
    dialect.getJDBCType(dt).orElse(getCommonJDBCType(dt)).getOrElse(
      throw new IllegalArgumentException(s"Can't get JDBC type for ${dt.catalogString}"))
  }

  private type JDBCValueSetter_add = (PreparedStatement, Row, Int, Int) => Unit

  private def makeSetter(conn: Connection, dataType: DataType): JDBCValueSetter_add = dataType match {
    case IntegerType =>
      (stmt: PreparedStatement, row: Row, pos: Int, currentpos: Int) =>
        stmt.setInt(pos + 1, row.getInt(pos - currentpos))

    case LongType =>
      (stmt: PreparedStatement, row: Row, pos: Int, currentpos: Int) =>
        stmt.setLong(pos + 1, row.getLong(pos - currentpos))

    case DoubleType =>
      (stmt: PreparedStatement, row: Row, pos: Int, currentpos: Int) =>
        stmt.setDouble(pos + 1, row.getDouble(pos - currentpos))

    case FloatType =>
      (stmt: PreparedStatement, row: Row, pos: Int, currentpos: Int) =>
        stmt.setFloat(pos + 1, row.getFloat(pos - currentpos))

    case ShortType =>
      (stmt: PreparedStatement, row: Row, pos: Int, currentpos: Int) =>
        stmt.setInt(pos + 1, row.getShort(pos - currentpos))

    case ByteType =>
      (stmt: PreparedStatement, row: Row, pos: Int, currentpos: Int) =>
        stmt.setInt(pos + 1, row.getByte(pos - currentpos))

    case BooleanType =>
      (stmt: PreparedStatement, row: Row, pos: Int, currentpos: Int) =>
        stmt.setBoolean(pos + 1, row.getBoolean(pos - currentpos))

    case StringType =>
      (stmt: PreparedStatement, row: Row, pos: Int, currentpos: Int) =>
        stmt.setString(pos + 1, row.getString(pos - currentpos))

    case BinaryType =>
      (stmt: PreparedStatement, row: Row, pos: Int, currentpos: Int) =>
        stmt.setBytes(pos + 1, row.getAs[Array[Byte]](pos - currentpos))

    case TimestampType =>
      (stmt: PreparedStatement, row: Row, pos: Int, currentpos: Int) =>
        stmt.setTimestamp(pos + 1, row.getAs[java.sql.Timestamp](pos - currentpos))

    case DateType =>
      (stmt: PreparedStatement, row: Row, pos: Int, currentpos: Int) =>
        stmt.setDate(pos + 1, row.getAs[java.sql.Date](pos - currentpos))

    case t: DecimalType =>
      (stmt: PreparedStatement, row: Row, pos: Int, currentpos: Int) =>
        stmt.setBigDecimal(pos + 1, row.getDecimal(pos - currentpos))
    case _ =>
      (stmt: PreparedStatement, row: Row, pos: Int, currentpos: Int) =>
        throw new IllegalArgumentException(
          s"Can't translate non-null value for field $pos")
  }
}
