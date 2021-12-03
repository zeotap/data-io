package com.zeotap.data.io.sink.beam.writer

import java.io.File
import java.sql.DriverManager

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.zeotap.data.io.common.test.helpers.DataFrameUtils.assertDataFrameEquality
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.AvroIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.values.PCollection
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.testcontainers.containers.PostgreSQLContainer

class JDBCBeamWriterTest extends FunSuite with DataFrameSuiteBase with BeforeAndAfterEach {

  val tempInputPath: String = "src/test/resources/temp/custom-input-format/yr=2021/mon=12/dt=02"
  val container = new PostgreSQLContainer("postgres:9.6.12")

  val testSchema = List(
    StructField("common_datapartnerid", IntegerType, true),
    StructField("deviceid", StringType, true),
    StructField("demographic_country", StringType, true),
    StructField("common_ts", StringType, true)
  )

  val schemaJson: String =
    """
      |{
      |  "type": "record",
      |  "name": "TEST_SCHEMA",
      |  "fields": [
      |    {
      |      "name": "common_datapartnerid",
      |      "type": "int",
      |      "default": 10
      |    },
      |    {
      |      "name": "deviceid",
      |      "type": "string",
      |      "default": "null"
      |    },
      |    {
      |      "name": "demographic_country",
      |      "type": "string",
      |      "default": "null"
      |    },
      |    {
      |      "name": "common_ts",
      |      "type": "string",
      |      "default": "null"
      |    }
      |  ]
      |}
      |""".stripMargin

  override def beforeAll(): Unit = {
    super.beforeAll()
    container.start()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    container.stop()
  }

  override def beforeEach(): Unit = createTable("org.postgresql.Driver", container.getJdbcUrl, container.getUsername, container.getPassword)

  override def afterEach(): Unit = {
    FileUtils.forceDelete(new File(tempInputPath))
    dropTable("org.postgresql.Driver", container.getJdbcUrl, container.getUsername, container.getPassword)
  }

  def getGenericRecordPCollection()(implicit beam: Pipeline): PCollection[GenericRecord] = {
    val sampleDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, "1", "India", "1504679559"),
        Row(1, "2", "India", "1504679359"),
        Row(1, "3", "Spain", "1504679459"),
        Row(1, "4", "India", "1504679659")
      )),
      StructType(testSchema)
    )

    sampleDf.write.format("avro").save(tempInputPath)
    beam.apply(AvroIO.readGenericRecords(schemaJson).from(tempInputPath + "/*.avro"))
  }

  def createTable(driverName: String, dbUrl: String, dbUserName: String, dbPassword: String): Unit = {
    Class.forName(driverName)
    val connection = DriverManager.getConnection(dbUrl, dbUserName, dbPassword)

    val query = "create table test_table (common_datapartnerid int, deviceid text, demographic_country text, common_ts text);"
    val statement =  connection.prepareStatement(query)
    statement.executeUpdate()
  }

  def insertRowsInTable(driverName: String, dbUrl: String, dbUserName: String, dbPassword: String): Unit = {
    Class.forName(driverName)
    val connection = DriverManager.getConnection(dbUrl, dbUserName, dbPassword)

    val insertQuery1 = "insert into test_table(common_datapartnerid, deviceid, demographic_country, common_ts) values (1, 'abc', 'abc', 'abc')"
    val insertQuery2 = "insert into test_table(common_datapartnerid, deviceid, demographic_country, common_ts) values (2, 'def', 'def', 'def')"
    val insertQuery3 = "insert into test_table(common_datapartnerid, deviceid, demographic_country, common_ts) values (3, 'ghi', 'ghi', 'ghi')"
    List(insertQuery1, insertQuery2, insertQuery3).foreach(query => {
      val insertStatement = connection.prepareStatement(query)
      insertStatement.executeUpdate()
    })
  }

  def dropTable(driverName: String, dbUrl: String, dbUserName: String, dbPassword: String): Unit = {
    Class.forName(driverName)
    val connection = DriverManager.getConnection(dbUrl, dbUserName, dbPassword)

    val query = "drop table if exists test_table;"
    val statement =  connection.prepareStatement(query)
    statement.executeUpdate()
  }

  test("JDBC write test") {
    implicit val beam: Pipeline = Pipeline.create(PipelineOptionsFactory.create)

    val pCollection = getGenericRecordPCollection()

    JDBCBeamWriter()
      .schema(schemaJson)
      .connectionProperties(container.getJdbcUrl, container.getUsername, container.getPassword)
      .driver("org.postgresql.Driver")
      .tableName("test_table")
      .save()
      .buildUnsafe(pCollection)

    beam.run()

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, "1", "India", "1504679559"),
        Row(1, "2", "India", "1504679359"),
        Row(1, "3", "Spain", "1504679459"),
        Row(1, "4", "India", "1504679659")
      )),
      StructType(testSchema)
    )

    val actualDf = spark.read.format("jdbc")
      .option("url", container.getJdbcUrl)
      .option("user", container.getUsername)
      .option("password", container.getPassword)
      .option("dbtable", "test_table")
      .load()

    assertDataFrameEquality(expectedDf, actualDf, "deviceid")
  }

  test("JDBC write test with existing rows") {
    implicit val beam: Pipeline = Pipeline.create(PipelineOptionsFactory.create)

    insertRowsInTable("org.postgresql.Driver", container.getJdbcUrl, container.getUsername, container.getPassword)
    val pCollection = getGenericRecordPCollection()

    JDBCBeamWriter()
      .schema(schemaJson)
      .connectionProperties(container.getJdbcUrl, container.getUsername, container.getPassword)
      .driver("org.postgresql.Driver")
      .tableName("test_table")
      .save()
      .buildUnsafe(pCollection)

    beam.run()

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, "abc", "abc", "abc"),
        Row(2, "def", "def", "def"),
        Row(3, "ghi", "ghi", "ghi"),
        Row(1, "1", "India", "1504679559"),
        Row(1, "2", "India", "1504679359"),
        Row(1, "3", "Spain", "1504679459"),
        Row(1, "4", "India", "1504679659")
      )),
      StructType(testSchema)
    )

    val actualDf = spark.read.format("jdbc")
      .option("url", container.getJdbcUrl)
      .option("user", container.getUsername)
      .option("password", container.getPassword)
      .option("dbtable", "test_table")
      .load()

    assertDataFrameEquality(expectedDf, actualDf, "deviceid")
  }

}
