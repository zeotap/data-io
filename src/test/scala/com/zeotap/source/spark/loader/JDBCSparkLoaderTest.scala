package com.zeotap.source.spark.loader

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.zeotap.source.spark.test.helpers.DataFrameUtils.assertDataFrameEquality
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite
import org.testcontainers.containers.{JdbcDatabaseContainer, MySQLContainer, PostgreSQLContainer}

import java.sql.DriverManager

class JDBCSparkLoaderTest extends FunSuite with DataFrameSuiteBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  def createTableAndInsertRows(driverName: String, dbUrl: String, dbUserName: String, dbPassword: String): Unit = {
    Class.forName(driverName)
    val connection = DriverManager.getConnection(dbUrl, dbUserName, dbPassword)

    val query = "create table test_table (id int, name text);"
    val statement =  connection.prepareStatement(query)
    statement.executeUpdate()

    val insertQuery1 = "insert into test_table(id, name) values (1, 'abc')"
    val insertQuery2 = "insert into test_table(id, name) values (2, 'def')"
    val insertQuery3 = "insert into test_table(id, name) values (3, 'ghi')"
    List(insertQuery1, insertQuery2, insertQuery3).foreach(query => {
      val insertStatement = connection.prepareStatement(query)
      insertStatement.executeUpdate()
    })
  }

  def jdbcLoadTest(container: JdbcDatabaseContainer[Nothing], driverName: String): Unit = {
    container.start()

    val dbUrl = container.getJdbcUrl
    val dbUserName = container.getUsername
    val dbPassword = container.getPassword
    createTableAndInsertRows(driverName, dbUrl, dbUserName, dbPassword)

    val expectedSchema = List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true)
    )

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"abc"),
        Row(2,"def"),
        Row(3,"ghi")
      )),
      StructType(expectedSchema)
    )

    val df = JDBCSparkLoader()
      .connectionProperties(dbUrl, dbUserName, dbPassword)
      .tableName("test_table")
      .load()
      .buildUnsafe(spark)

    assertDataFrameEquality(expectedDf, df, "id")
    container.stop()
  }

  test("testForLoadPostgreSQL") {
    val container = new PostgreSQLContainer("postgres:9.6.12")
    jdbcLoadTest(container, "org.postgresql.Driver")
  }

  test("testForLoadMySQL") {
    val container = new MySQLContainer("mysql:8.0.26")
    jdbcLoadTest(container, "com.mysql.cj.jdbc.Driver")
  }

  test("testForCustomSchema") {
    val container = new PostgreSQLContainer("postgres:9.6.12")
    container.start()

    val dbUrl = container.getJdbcUrl
    val dbUserName = container.getUsername
    val dbPassword = container.getPassword
    createTableAndInsertRows("org.postgresql.Driver", dbUrl, dbUserName, dbPassword)

    val insertedSchema = List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true)
    )

    val insertedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"abc"),
        Row(2,"def"),
        Row(3,"ghi")
      )),
      StructType(insertedSchema)
    )

    val expectedDf = insertedDf.withColumn("id", col("id").cast(StringType))

    val df = JDBCSparkLoader()
      .customSchema("id string, name string")
      .connectionProperties(dbUrl, dbUserName, dbPassword)
      .query("select * from test_table")
      .load()
      .buildUnsafe(spark)

    assertDataFrameEquality(expectedDf, df, "id")
    container.stop()
  }

}
