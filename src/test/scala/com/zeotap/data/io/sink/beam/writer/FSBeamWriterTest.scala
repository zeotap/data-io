package com.zeotap.data.io.sink.beam.writer

import java.io.File

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.zeotap.data.io.common.test.helpers.DataFrameUtils.assertDataFrameEquality
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.AvroIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.values.PCollection
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class FSBeamWriterTest extends FunSuite with DataFrameSuiteBase with BeforeAndAfterEach {

  val tempInputPath: String = "src/test/resources/temp/custom-input-format/yr=2021/mon=12/dt=02"
  val outputPath: String = "src/test/resources/custom-output-format/yr=2021/mon=12/dt=02"

  val testSchema = List(
    StructField("Common_DataPartnerID", IntegerType, true),
    StructField("DeviceId", StringType, true),
    StructField("Demographic_Country", StringType, true),
    StructField("Common_TS", StringType, true)
  )

  val schemaJson: String =
    """
      |{
      |  "type": "record",
      |  "name": "TEST_SCHEMA",
      |  "fields": [
      |    {
      |      "name": "Common_DataPartnerID",
      |      "type": "int",
      |      "default": 10
      |    },
      |    {
      |      "name": "DeviceId",
      |      "type": "string",
      |      "default": "null"
      |    },
      |    {
      |      "name": "Demographic_Country",
      |      "type": "string",
      |      "default": "null"
      |    },
      |    {
      |      "name": "Common_TS",
      |      "type": "string",
      |      "default": "null"
      |    }
      |  ]
      |}
      |""".stripMargin

  override def beforeAll(): Unit = super.beforeAll()

  override def afterAll(): Unit = super.afterAll()

  override def afterEach(): Unit = {
    FileUtils.forceDelete(new File(tempInputPath))
    FileUtils.forceDelete(new File(outputPath))
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

  def assertExpectedDFEqualsSavedPCollection(actualDf: DataFrame): Unit = {
    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, "1", "India", "1504679559"),
        Row(1, "2", "India", "1504679359"),
        Row(1, "3", "Spain", "1504679459"),
        Row(1, "4", "India", "1504679659")
      )),
      StructType(testSchema)
    )

    assertDataFrameEquality(expectedDf, actualDf, "DeviceId")
  }

  test("Avro write test") {
    implicit val beam: Pipeline = Pipeline.create(PipelineOptionsFactory.create)
    val pCollection = getGenericRecordPCollection()

    AvroBeamWriter().schema(schemaJson).save(outputPath).buildUnsafe(pCollection)
    beam.run()

    assertExpectedDFEqualsSavedPCollection(spark.read.format("avro").load(outputPath))
  }

  test("Text write test") {
    implicit val beam: Pipeline = Pipeline.create(PipelineOptionsFactory.create)
    val pCollection = getGenericRecordPCollection()

    TextBeamWriter().schema(schemaJson).save(outputPath).buildUnsafe(pCollection)
    beam.run()

    val actualDf = spark.read.schema(StructType(testSchema)).option("inferSchema", "true").format("csv").load(outputPath)
      .toDF(Seq("Common_DataPartnerID", "DeviceId", "Demographic_Country", "Common_TS"): _*)
    assertExpectedDFEqualsSavedPCollection(actualDf)
  }

  test("CSV write test") {
    implicit val beam: Pipeline = Pipeline.create(PipelineOptionsFactory.create)
    val pCollection = getGenericRecordPCollection()

    CSVBeamWriter().schema(schemaJson).save(outputPath).buildUnsafe(pCollection)
    beam.run()

    val actualDf = spark.read.schema(StructType(testSchema)).option("inferSchema", "true").format("csv").load(outputPath)
      .toDF(Seq("Common_DataPartnerID", "DeviceId", "Demographic_Country", "Common_TS"): _*)
    assertExpectedDFEqualsSavedPCollection(actualDf)
  }

  test("JSON write test") {
    implicit val beam: Pipeline = Pipeline.create(PipelineOptionsFactory.create)
    val pCollection = getGenericRecordPCollection()

    JSONBeamWriter().schema(schemaJson).save(outputPath).buildUnsafe(pCollection)
    beam.run()

    assertExpectedDFEqualsSavedPCollection(spark.read.schema(StructType(testSchema)).format("json").load(outputPath))
  }

  test("Parquet write test") {
    implicit val beam: Pipeline = Pipeline.create(PipelineOptionsFactory.create)
    val pCollection = getGenericRecordPCollection()

    ParquetBeamWriter().schema(schemaJson).save(outputPath).buildUnsafe(pCollection)
    beam.run()

    assertExpectedDFEqualsSavedPCollection(spark.read.format("parquet").load(outputPath))
  }

}
