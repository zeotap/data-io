package com.zeotap.data.io.source.beam.loader

import java.io.File
import java.sql.DriverManager

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.zeotap.data.io.common.test.helpers.PCollectionUtils.assertPCollectionEqualsDataFrame
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.testcontainers.containers.{JdbcDatabaseContainer, MySQLContainer, PostgreSQLContainer}

class JDBCBeamLoaderTest extends FunSuite with DataFrameSuiteBase with BeforeAndAfterEach {

  val tempPath: String = "src/test/resources/custom-output-format/yr=2021/mon=12/dt=02"

  override def beforeAll(): Unit = super.beforeAll()

  override def afterAll(): Unit = super.afterAll()

  override def afterEach(): Unit = FileUtils.forceDelete(new File(tempPath))

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

    val schemaJson =
      """
        |{
        |  "type": "record",
        |  "name": "TEST_SCHEMA",
        |  "fields": [
        |    {
        |      "name": "id",
        |      "type": "int",
        |      "default": 10
        |    },
        |    {
        |      "name": "name",
        |      "type": "string",
        |      "default": "null"
        |    }
        |  ]
        |}
        |""".stripMargin

    implicit val beam: Pipeline = Pipeline.create(PipelineOptionsFactory.create)

    val pCollection = JDBCBeamLoader()
      .schema(schemaJson)
      .connectionProperties(dbUrl, dbUserName, dbPassword)
      .driver(driverName)
      .tableName("test_table")
      .load()
      .buildUnsafe

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

    assertPCollectionEqualsDataFrame(expectedDf, pCollection, schemaJson, "id", tempPath)
    container.stop()
  }

  test("JDBC read test - PostgreSQL") {
    val container = new PostgreSQLContainer("postgres:9.6.12")
    jdbcLoadTest(container, "org.postgresql.Driver")
  }

  test("JDBC read test - MySQL") {
    val container = new MySQLContainer("mysql:8.0.26")
    jdbcLoadTest(container, "com.mysql.cj.jdbc.Driver")
  }

}
