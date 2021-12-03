package com.zeotap.data.io.source.beam.loader

import java.io.File

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.zeotap.data.io.common.test.helpers.CommonUtils.writeToFile
import com.zeotap.data.io.common.test.helpers.PCollectionUtils.assertPCollectionEqualsDataFrame
import com.zeotap.data.io.common.types.{Int, OptionalColumn, String}
import com.zeotap.data.io.helpers.beam.BeamHelpers
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.collection.JavaConverters._

class FSBeamLoaderTest extends FunSuite with DataFrameSuiteBase with BeforeAndAfterEach {

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

  val testSchema = List(
    StructField("Common_DataPartnerID", IntegerType, true),
    StructField("DeviceId", StringType, true),
    StructField("Demographic_Country", StringType, true),
    StructField("Common_TS", StringType, true)
  )

  val inputPath : String = "src/test/resources/custom-input-format/yr=2021/mon=12/dt=02"
  val outputPath : String = "src/test/resources/custom-output-format/yr=2021/mon=12/dt=02"

  override def beforeAll(): Unit = super.beforeAll()

  override def afterEach(): Unit = {
    FileUtils.forceDelete(new File(inputPath))
    FileUtils.forceDelete(new File(outputPath))
  }

  override def afterAll(): Unit = super.afterAll()

  def writeDFToInputPath(format: String, path: String): Unit = {
    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559"),
        Row(1,"2","India","1504679359"),
        Row(1,"3","Spain","1504679459"),
        Row(1,"4","India","1504679659")
      )),
      StructType(testSchema)
    ).write.format(format).save(path)
  }

  def writeCSVDataToInputPath(format: String, path: String): Unit = {
    val commaSeparatedString =
      """
        |a,b,1,2
        |c,d,3,4
        |e,f,5,6
        |""".stripMargin

    FileUtils.forceMkdir(new File(inputPath))
    writeToFile(inputPath + f"/a.$format", commaSeparatedString)
  }

  test("Avro read test") {
    implicit val beam: Pipeline = Pipeline.create(PipelineOptionsFactory.create)

    writeDFToInputPath("avro", inputPath)

    val pCollection = AvroBeamLoader()
      .schema(schemaJson)
      .load(inputPath)
      .buildUnsafe

    val expectedDf = spark.read.format("avro").load(inputPath)
    assertPCollectionEqualsDataFrame(expectedDf, pCollection, schemaJson, "DeviceId", outputPath)
  }

  test("Text read test") {
    implicit val beam: Pipeline = Pipeline.create(PipelineOptionsFactory.create)

    writeCSVDataToInputPath("txt", inputPath)

    val schemaJson =
      """
        |{
        |  "type": "record",
        |  "name": "TEST_SCHEMA",
        |  "fields": [
        |    {
        |      "name": "_c0",
        |      "type": "string",
        |      "default": "10"
        |    },
        |    {
        |      "name": "_c1",
        |      "type": "string",
        |      "default": "10"
        |    },
        |    {
        |      "name": "_c2",
        |      "type": "string",
        |      "default": "10"
        |    },
        |    {
        |      "name": "_c3",
        |      "type": "string",
        |      "default": "10"
        |    }
        |  ]
        |}
        |""".stripMargin

    val pCollection = TextBeamLoader()
      .schema(schemaJson)
      .load(inputPath)
      .buildUnsafe

    val expectedDf = spark.read.format("csv").option("delimiter", ",").load(inputPath)
    assertPCollectionEqualsDataFrame(expectedDf, pCollection, schemaJson, "_c2", outputPath)
  }

  test("CSV read test") {
    implicit val beam: Pipeline = Pipeline.create(PipelineOptionsFactory.create)

    writeCSVDataToInputPath("csv", inputPath)

    val schemaJson =
      """
        |{
        |  "type": "record",
        |  "name": "TEST_SCHEMA",
        |  "fields": [
        |    {
        |      "name": "_c0",
        |      "type": "string",
        |      "default": "10"
        |    },
        |    {
        |      "name": "_c1",
        |      "type": "string",
        |      "default": "10"
        |    },
        |    {
        |      "name": "_c2",
        |      "type": "string",
        |      "default": "10"
        |    },
        |    {
        |      "name": "_c3",
        |      "type": "string",
        |      "default": "10"
        |    }
        |  ]
        |}
        |""".stripMargin

    val pCollection = CSVBeamLoader()
      .schema(schemaJson)
      .load(inputPath)
      .buildUnsafe

    val expectedDf = spark.read.format("csv").load(inputPath)
    assertPCollectionEqualsDataFrame(expectedDf, pCollection, schemaJson, "_c2", outputPath)
  }

  test("JSON read test") {
    implicit val beam: Pipeline = Pipeline.create(PipelineOptionsFactory.create)

    writeDFToInputPath("json", inputPath)

    val pCollection = JSONBeamLoader()
      .schema(schemaJson)
      .load(inputPath)
      .buildUnsafe

    val expectedDf = spark.read.schema(StructType(testSchema)).format("json").load(inputPath)
    assertPCollectionEqualsDataFrame(expectedDf, pCollection, schemaJson, "DeviceId", outputPath)
  }

  test("Parquet read test") {
    implicit val beam: Pipeline = Pipeline.create(PipelineOptionsFactory.create)

    writeDFToInputPath("parquet", inputPath)

    val pCollection = ParquetBeamLoader()
      .schema(schemaJson)
      .load(inputPath)
      .buildUnsafe

    val expectedDf = spark.read.format("parquet").load(inputPath)
    assertPCollectionEqualsDataFrame(expectedDf, pCollection, schemaJson, "DeviceId", outputPath)
  }

  test("Avro read test with optional columns") {
    implicit val beam: Pipeline = Pipeline.create(PipelineOptionsFactory.create)

    writeDFToInputPath("avro", inputPath)
    
    val optionalColumns = List(OptionalColumn("newCol", "1", Int), OptionalColumn("newCol2", "2", String))
    val schemaWithOptionalColumns = BeamHelpers.addOptionalColumnsToSchema(schemaJson, optionalColumns.asJava)

    val pCollection = AvroBeamLoader()
      .schema(schemaJson)
      .load(inputPath)
      .addOptionalColumns(optionalColumns)
      .buildUnsafe

    val expectedDf = spark.read.format("avro").load(inputPath).withColumn("newCol", lit(1)).withColumn("newCol2", lit("2"))
    assertPCollectionEqualsDataFrame(expectedDf, pCollection, schemaWithOptionalColumns, "DeviceId", outputPath)
  }

  test("Text read test with optional columns") {
    implicit val beam: Pipeline = Pipeline.create(PipelineOptionsFactory.create)

    writeCSVDataToInputPath("txt", inputPath)

    val schemaJson =
      """
        |{
        |  "type": "record",
        |  "name": "TEST_SCHEMA",
        |  "fields": [
        |    {
        |      "name": "_c0",
        |      "type": "string",
        |      "default": "10"
        |    },
        |    {
        |      "name": "_c1",
        |      "type": "string",
        |      "default": "10"
        |    },
        |    {
        |      "name": "_c2",
        |      "type": "string",
        |      "default": "10"
        |    },
        |    {
        |      "name": "_c3",
        |      "type": "string",
        |      "default": "10"
        |    }
        |  ]
        |}
        |""".stripMargin

    val optionalColumns = List(OptionalColumn("newCol", "1", Int), OptionalColumn("newCol2", "2", String))
    val schemaWithOptionalColumns = BeamHelpers.addOptionalColumnsToSchema(schemaJson, optionalColumns.asJava)

    val pCollection = TextBeamLoader()
      .schema(schemaJson)
      .load(inputPath)
      .addOptionalColumns(optionalColumns)
      .buildUnsafe

    val expectedDf = spark.read.format("csv").option("delimiter", ",").load(inputPath).withColumn("newCol", lit(1)).withColumn("newCol2", lit("2"))
    assertPCollectionEqualsDataFrame(expectedDf, pCollection, schemaWithOptionalColumns, "_c2", outputPath)
  }

  test("CSV read test with optional columns") {
    implicit val beam: Pipeline = Pipeline.create(PipelineOptionsFactory.create)

    writeCSVDataToInputPath("csv", inputPath)

    val schemaJson =
      """
        |{
        |  "type": "record",
        |  "name": "TEST_SCHEMA",
        |  "fields": [
        |    {
        |      "name": "_c0",
        |      "type": "string",
        |      "default": "10"
        |    },
        |    {
        |      "name": "_c1",
        |      "type": "string",
        |      "default": "10"
        |    },
        |    {
        |      "name": "_c2",
        |      "type": "string",
        |      "default": "10"
        |    },
        |    {
        |      "name": "_c3",
        |      "type": "string",
        |      "default": "10"
        |    }
        |  ]
        |}
        |""".stripMargin

    val optionalColumns = List(OptionalColumn("newCol", "1", Int), OptionalColumn("newCol2", "2", String))
    val schemaWithOptionalColumns = BeamHelpers.addOptionalColumnsToSchema(schemaJson, optionalColumns.asJava)

    val pCollection = CSVBeamLoader()
      .schema(schemaJson)
      .load(inputPath)
      .addOptionalColumns(optionalColumns)
      .buildUnsafe

    val expectedDf = spark.read.format("csv").load(inputPath).withColumn("newCol", lit(1)).withColumn("newCol2", lit("2"))
    assertPCollectionEqualsDataFrame(expectedDf, pCollection, schemaWithOptionalColumns, "_c2", outputPath)
  }

  test("JSON read test with optional columns") {
    implicit val beam: Pipeline = Pipeline.create(PipelineOptionsFactory.create)

    writeDFToInputPath("json", inputPath)

    val optionalColumns = List(OptionalColumn("newCol", "1", Int), OptionalColumn("newCol2", "2", String))
    val schemaWithOptionalColumns = BeamHelpers.addOptionalColumnsToSchema(schemaJson, optionalColumns.asJava)

    val pCollection = JSONBeamLoader()
      .schema(schemaJson)
      .load(inputPath)
      .addOptionalColumns(optionalColumns)
      .buildUnsafe

    val expectedDf = spark.read.schema(StructType(testSchema)).format("json").load(inputPath).withColumn("newCol", lit(1)).withColumn("newCol2", lit("2"))
    assertPCollectionEqualsDataFrame(expectedDf, pCollection, schemaWithOptionalColumns, "DeviceId", outputPath)
  }

  test("Parquet read test with optional columns") {
    implicit val beam: Pipeline = Pipeline.create(PipelineOptionsFactory.create)

    writeDFToInputPath("parquet", inputPath)

    val optionalColumns = List(OptionalColumn("newCol", "1", Int), OptionalColumn("newCol2", "2", String))
    val schemaWithOptionalColumns = BeamHelpers.addOptionalColumnsToSchema(schemaJson, optionalColumns.asJava)

    val pCollection = ParquetBeamLoader()
      .schema(schemaJson)
      .load(inputPath)
      .addOptionalColumns(optionalColumns)
      .buildUnsafe

    val expectedDf = spark.read.format("parquet").load(inputPath).withColumn("newCol", lit(1)).withColumn("newCol2", lit("2"))
    assertPCollectionEqualsDataFrame(expectedDf, pCollection, schemaWithOptionalColumns, "DeviceId", outputPath)
  }

}
