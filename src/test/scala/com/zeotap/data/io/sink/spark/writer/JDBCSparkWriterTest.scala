package com.zeotap.data.io.sink.spark.writer

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.zeotap.data.io.common.types.{Append, ErrorIfExists, Ignore, Overwrite}
import com.zeotap.data.io.common.test.helpers.DataFrameUtils.{assertDataFrameEquality, unionByName}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.testcontainers.containers.PostgreSQLContainer

import java.sql.DriverManager

class JDBCSparkWriterTest extends FunSuite with BeforeAndAfterEach with DataFrameSuiteBase {

  val container = new PostgreSQLContainer("postgres:9.6.12")

  override def beforeAll(): Unit = {
    super.beforeAll()
    container.start()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    container.stop()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    dropTable(container.getJdbcUrl, container.getUsername, container.getPassword, "org.postgresql.Driver")
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
      .addSaveMode(ErrorIfExists)
      .save()
      .buildSafe(df2)

    assert(eitherExceptionOrSave.isLeft)
    assert(!eitherExceptionOrSave.isRight)
    assert(eitherExceptionOrSave.left.get.contains("already exists"))
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
      .addSaveMode(Ignore)
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
      .addSaveMode(Append)
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

    assertDataFrameEquality(unionByName(df1, df2), actualDf, "id")
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
      .addSaveMode(Overwrite)
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
  }

}
