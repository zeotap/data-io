package com.zeotap.source.spark.loader

import com.fasterxml.jackson.databind.ObjectMapper
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.zeotap.source.spark.test.helpers.DataFrameUtils.assertDataFrameEquality
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite

import java.io.File

class AvroSparkLoaderTest extends FunSuite with DataFrameSuiteBase {

  val inputAvroPath1 : String = "src/test/resources/custom-input-format/yr=2021/mon=07/dt=19"
  val inputAvroPath2 : String = "src/test/resources/custom-input-format/yr=2021/mon=07/dt=18"
  val inputAvroPath3 : String = "src/test/resources/custom-input-format/yr=2021/mon=07/dt=17"

  override def beforeAll(): Unit = {
    super.beforeAll()

    val testSchema = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true)
    )

    val sampleDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559"),
        Row(1,"2","India","1504679359"),
        Row(1,"3","Spain","1504679459"),
        Row(1,"4","India","1504679659")
      )),
      StructType(testSchema)
    )

    sampleDf.write.format("avro").save(inputAvroPath1)

    val testSchema2 = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true),
      StructField("DeviceId", StringType, true)
    )

    val sampleDf2 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"France","1504679559","5"),
        Row(1,"Germany","1504679359","6"),
        Row(1,"Italy","1504679459","7"),
        Row(1,"Belgium","1504679659","8")
      )),
      StructType(testSchema2)
    )

    sampleDf2.write.format("avro").save(inputAvroPath2)

    val testSchema3 = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Gender", StringType, true)
    )

    val sampleDf3 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"France","1504679559","5","Male"),
        Row(1,"Germany","1504679359","6","Female"),
        Row(1,"Italy","1504679459","7","Female"),
        Row(1,"Belgium","1504679659","8","Male")
      )),
      StructType(testSchema3)
    )

    sampleDf3.write.format("avro").save(inputAvroPath3)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    FileUtils.forceDelete(new File(inputAvroPath1))
    FileUtils.forceDelete(new File(inputAvroPath2))
    FileUtils.forceDelete(new File(inputAvroPath3))
  }

  test("testForLoadPath") {
    val expectedSchema = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true)
    )

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559"),
        Row(1,"2","India","1504679359"),
        Row(1,"3","Spain","1504679459"),
        Row(1,"4","India","1504679659")
      )),
      StructType(expectedSchema)
    )

    val df = AvroSparkLoader()
      .load(inputAvroPath1)
      .build(spark)

    assertDataFrameEquality(expectedDf, df, "DeviceId")
  }

  test("testForCustomSchema") {
    val insertedSchema = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true)
    )

    val insertedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559"),
        Row(1,"2","India","1504679359"),
        Row(1,"3","Spain","1504679459"),
        Row(1,"4","India","1504679659")
      )),
      StructType(insertedSchema)
    )

    val expectedDf = insertedDf.drop("Common_TS")

    val schema = """
                   |{
                   |  "type": "record",
                   |  "name": "TEST_SCHEMA",
                   |  "fields": [
                   |    {
                   |      "name": "Common_DataPartnerID",
                   |      "type": [
                   |        "null",
                   |        "int"
                   |      ],
                   |      "default": null
                   |    },
                   |    {
                   |      "name": "DeviceId",
                   |      "type": [
                   |        "null",
                   |        "string"
                   |      ],
                   |      "default": null
                   |    },
                   |    {
                   |      "name": "Demographic_Country",
                   |      "type": [
                   |        "null",
                   |        "string"
                   |      ],
                   |      "default": null
                   |    }
                   |  ]
                   |}
                   |""".stripMargin

    val df = AvroSparkLoader()
      .avroSchema(schema)
      .load(inputAvroPath1)
      .build(spark)

    assertDataFrameEquality(expectedDf, df, "DeviceId")
  }

  test("testForCustomSchema2") {
    val insertedSchema = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true)
    )

    val insertedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559"),
        Row(1,"2","India","1504679359"),
        Row(1,"3","Spain","1504679459"),
        Row(1,"4","India","1504679659")
      )),
      StructType(insertedSchema)
    )

    val expectedDf = insertedDf.drop("Common_TS").drop("Demographic_Country")

    val schema = """
                   |{
                   |  "type": "record",
                   |  "name": "TEST_SCHEMA",
                   |  "fields": [
                   |    {
                   |      "name": "Common_DataPartnerID",
                   |      "type": [
                   |        "null",
                   |        "int"
                   |      ],
                   |      "default": null
                   |    },
                   |    {
                   |      "name": "DeviceId",
                   |      "type": [
                   |        "null",
                   |        "string"
                   |      ],
                   |      "default": null
                   |    }
                   |  ]
                   |}
                   |""".stripMargin

    val df = AvroSparkLoader()
      .avroSchema(new ObjectMapper().readTree(schema))
      .load(inputAvroPath1)
      .build(spark)

    assertDataFrameEquality(expectedDf, df, "DeviceId")
  }

}
