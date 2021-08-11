package com.zeotap.sink.spark.writer

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.zeotap.common.types.{APPEND, ERROR_IF_EXISTS, IGNORE, OVERWRITE}
import com.zeotap.test.helpers.DataFrameUtils.{assertDataFrameEquality, safeColumnUnion}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite
import org.testcontainers.containers.PostgreSQLContainer

import java.sql.DriverManager

class JDBCSparkWriterTest extends FunSuite with DataFrameSuiteBase {

  val container = new PostgreSQLContainer("postgres:9.6.12")

  override def beforeAll(): Unit = {
    super.beforeAll()
    container.start()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    container.stop()
  }

  def dropTable(dbUrl: String, dbUserName: String, dbPassword: String, driverName: String): Unit = {
    Class.forName(driverName)
    val connection = DriverManager.getConnection(dbUrl, dbUserName, dbPassword)

    val query = "drop table if exists test_table;"
    val statement =  connection.prepareStatement(query)
    statement.executeUpdate()
  }

  test("basicWriteTest") {
    val schema = List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true)
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"abc"),
        Row(2,"def"),
        Row(3,"ghi")
      )),
      StructType(schema)
    )

    val eitherExceptionOrSave = JDBCSparkWriter()
      .connectionProperties(container.getJdbcUrl, container.getUsername, container.getPassword)
      .tableName("test_table")
      .save()
      .buildSafe(df)

    assert(!eitherExceptionOrSave.isLeft)
    assert(eitherExceptionOrSave.isRight)

    val actualDf = spark.read.format("jdbc")
      .option("url", container.getJdbcUrl)
      .option("user", container.getUsername)
      .option("password", container.getPassword)
      .option("dbtable", "test_table")
      .load()

    assertDataFrameEquality(df, actualDf, "id")
    dropTable(container.getJdbcUrl, container.getUsername, container.getPassword, "org.postgresql.Driver")
  }

  test("errorIfExistsTest") {
    val schema = List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true)
    )

    val df1 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"abc"),
        Row(2,"def"),
        Row(3,"ghi")
      )),
      StructType(schema)
    )

    val df2 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(4,"jkl"),
        Row(5,"mno"),
        Row(6,"pqr")
      )),
      StructType(schema)
    )

    df1.write.format("jdbc")
      .option("url", container.getJdbcUrl)
      .option("user", container.getUsername)
      .option("password", container.getPassword)
      .option("dbtable", "test_table")
      .save()


    val eitherExceptionOrSave = JDBCSparkWriter()
      .connectionProperties(container.getJdbcUrl, container.getUsername, container.getPassword)
      .tableName("test_table")
      .addSaveMode(ERROR_IF_EXISTS)
      .save()
      .buildSafe(df2)

    assert(eitherExceptionOrSave.isLeft)
    assert(!eitherExceptionOrSave.isRight)
    assert(eitherExceptionOrSave.left.get.contains("already exists"))
    dropTable(container.getJdbcUrl, container.getUsername, container.getPassword, "org.postgresql.Driver")
  }

  test("ignoreTest") {
    val schema = List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true)
    )

    val df1 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"abc"),
        Row(2,"def"),
        Row(3,"ghi")
      )),
      StructType(schema)
    )

    val df2 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(4,"jkl"),
        Row(5,"mno"),
        Row(6,"pqr")
      )),
      StructType(schema)
    )

    df1.write.format("jdbc")
      .option("url", container.getJdbcUrl)
      .option("user", container.getUsername)
      .option("password", container.getPassword)
      .option("dbtable", "test_table")
      .save()


    val eitherExceptionOrSave = JDBCSparkWriter()
      .connectionProperties(container.getJdbcUrl, container.getUsername, container.getPassword)
      .tableName("test_table")
      .addSaveMode(IGNORE)
      .save()
      .buildSafe(df2)

    assert(!eitherExceptionOrSave.isLeft)
    assert(eitherExceptionOrSave.isRight)

    val actualDf = spark.read.format("jdbc")
      .option("url", container.getJdbcUrl)
      .option("user", container.getUsername)
      .option("password", container.getPassword)
      .option("dbtable", "test_table")
      .load()

    assertDataFrameEquality(df1, actualDf, "id")
    dropTable(container.getJdbcUrl, container.getUsername, container.getPassword, "org.postgresql.Driver")
  }

  test("appendTest") {
    val schema = List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true)
    )

    val df1 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"abc"),
        Row(2,"def"),
        Row(3,"ghi")
      )),
      StructType(schema)
    )

    val df2 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(4,"jkl"),
        Row(5,"mno"),
        Row(6,"pqr")
      )),
      StructType(schema)
    )

    df1.write.format("jdbc")
      .option("url", container.getJdbcUrl)
      .option("user", container.getUsername)
      .option("password", container.getPassword)
      .option("dbtable", "test_table")
      .save()


    val eitherExceptionOrSave = JDBCSparkWriter()
      .connectionProperties(container.getJdbcUrl, container.getUsername, container.getPassword)
      .tableName("test_table")
      .addSaveMode(APPEND)
      .save()
      .buildSafe(df2)

    assert(!eitherExceptionOrSave.isLeft)
    assert(eitherExceptionOrSave.isRight)

    val actualDf = spark.read.format("jdbc")
      .option("url", container.getJdbcUrl)
      .option("user", container.getUsername)
      .option("password", container.getPassword)
      .option("dbtable", "test_table")
      .load()

    assertDataFrameEquality(safeColumnUnion(df1, df2), actualDf, "id")
    dropTable(container.getJdbcUrl, container.getUsername, container.getPassword, "org.postgresql.Driver")
  }

  test("overwriteTest") {
    val schema = List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true)
    )

    val df1 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"abc"),
        Row(2,"def"),
        Row(3,"ghi")
      )),
      StructType(schema)
    )

    val df2 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(4,"jkl"),
        Row(5,"mno"),
        Row(6,"pqr")
      )),
      StructType(schema)
    )

    df1.write.format("jdbc")
      .option("url", container.getJdbcUrl)
      .option("user", container.getUsername)
      .option("password", container.getPassword)
      .option("dbtable", "test_table")
      .save()


    val eitherExceptionOrSave = JDBCSparkWriter()
      .connectionProperties(container.getJdbcUrl, container.getUsername, container.getPassword)
      .tableName("test_table")
      .addSaveMode(OVERWRITE)
      .save()
      .buildSafe(df2)

    assert(!eitherExceptionOrSave.isLeft)
    assert(eitherExceptionOrSave.isRight)

    val actualDf = spark.read.format("jdbc")
      .option("url", container.getJdbcUrl)
      .option("user", container.getUsername)
      .option("password", container.getPassword)
      .option("dbtable", "test_table")
      .load()

    assertDataFrameEquality(df2, actualDf, "id")
    dropTable(container.getJdbcUrl, container.getUsername, container.getPassword, "org.postgresql.Driver")
  }

}
