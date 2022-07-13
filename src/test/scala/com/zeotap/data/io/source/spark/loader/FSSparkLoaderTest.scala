package com.zeotap.data.io.source.spark.loader

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.zeotap.data.io.common.test.helpers.DataFrameUtils.assertDataFrameEquality
import com.zeotap.data.io.common.types._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite

import java.io.File

class FSSparkLoaderTest extends FunSuite with DataFrameSuiteBase {

  val inputOrcPath: String = "src/test/resources/custom-input-format/orc"
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

    sampleDf.write.format("orc").save(inputOrcPath)
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
    FileUtils.forceDelete(new File(inputOrcPath))
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

    val df = new FSSparkLoader()
      .addFormat(ORC)
      .load(inputOrcPath)
      .buildUnsafe(spark)

    assertDataFrameEquality(expectedDf, df, "DeviceId")
  }

  test("testForLookBack") {
    val expectedSchema = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true),
      StructField("Demographic_Gender", StringType, true)
    )

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559",null),
        Row(1,"2","India","1504679359",null),
        Row(1,"3","Spain","1504679459",null),
        Row(1,"4","India","1504679659",null),
        Row(1,"5","France","1504679559",null),
        Row(1,"6","Germany","1504679359",null),
        Row(1,"7","Italy","1504679459",null),
        Row(1,"8","Belgium","1504679659",null),
        Row(1,"5","France","1504679559","Male"),
        Row(1,"6","Germany","1504679359","Female"),
        Row(1,"7","Italy","1504679459","Female"),
        Row(1,"8","Belgium","1504679659","Male")
      )),
      StructType(expectedSchema)
    )

    val df = new FSSparkLoader()
      .addFormat(Avro)
      .lookBack("src/test/resources/custom-input-format/yr=${YR}/mon=${MON}/dt=${DT}", Map("YR" -> "2021", "MON" -> "07", "DT" -> "19"), 3)
      .buildUnsafe(spark)

    assertDataFrameEquality(expectedDf, df, "DeviceId")
  }

  test("testForLatestPath") {
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

    val df = new FSSparkLoader()
      .addFormat(Avro)
      .latestPaths("src/test/resources/custom-input-format/yr=${YR}/mon=${MON}/dt=${DT}", Map("YR" -> "2021", "MON" -> "07", "DT" -> "17"), false)
      .buildUnsafe(spark)

    assertDataFrameEquality(expectedDf, df, "DeviceId")
  }

  test("testForOptionalColumn") {
    val expectedSchema = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true),
      StructField("New_Column", StringType, false)
    )

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559","1234"),
        Row(1,"2","India","1504679359","1234"),
        Row(1,"3","Spain","1504679459","1234"),
        Row(1,"4","India","1504679659","1234")
      )),
      StructType(expectedSchema)
    )

    val df = new FSSparkLoader()
      .addFormat(Avro)
      .load(inputAvroPath1)
      .addOptionalColumns(List(OptionalColumn("New_Column", "1234", String)))
      .buildUnsafe(spark)

    assertDataFrameEquality(expectedDf, df, "DeviceId")
  }

  test("testForMultipleDataFrameOptions") {
    val expectedSchema = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true),
      StructField("New_Column", StringType, false),
      StructField("New_Column2", IntegerType, true)
    )

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559","1234",5678),
        Row(1,"2","India","1504679359","1234",5678),
        Row(1,"3","Spain","1504679459","1234",5678),
        Row(1,"4","India","1504679659","1234",5678)
      )),
      StructType(expectedSchema)
    )

    val df = new FSSparkLoader()
      .addFormat(Avro)
      .load(inputAvroPath1)
      .addOptionalColumns(List(OptionalColumn("New_Column", "1234", String)))
      .addOptionalColumns(List(OptionalColumn("New_Column2", "5678", Int)))
      .buildUnsafe(spark)

    assertDataFrameEquality(expectedDf, df, "DeviceId")
  }

  test("testForMultiPathsWithCustomSchema") {
    val schema = StructType(
      List(
        StructField("Common_DataPartnerID", IntegerType, true),
        StructField("DeviceId", StringType, true),
        StructField("Demographic_Country", StringType, true),
        StructField("Common_TS", StringType, true)
      )
    )

    val schemaJson = schema.json

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559"),
        Row(1,"2","India","1504679359"),
        Row(1,"3","Spain","1504679459"),
        Row(1,"4","India","1504679659"),
        Row(1,"5","France","1504679559"),
        Row(1,"6","Germany","1504679359"),
        Row(1,"7","Italy","1504679459"),
        Row(1,"8","Belgium","1504679659")
      )),
      schema
    )

    val df = new FSSparkLoader()
      .addFormat(Avro)
      .schema(schemaJson)
      .load(List(inputAvroPath1, inputAvroPath2, inputAvroPath3))
      .buildUnsafe(spark)

    assertDataFrameEquality(expectedDf, df, "DeviceId")
  }

  test("Test for multiple options with split") {
    val expectedSchema = List(
      StructField("Common_DataPartnerID", IntegerType, nullable = true),
      StructField("DeviceId", StringType, nullable = true),
      StructField("Demographic_Country", StringType, nullable = true),
      StructField("Common_TS", StringType, nullable = true),
      StructField("Demographic_Gender", StringType, nullable = true)
    )

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, "1", "India", "1504679559", null),
        Row(1, "2", "India", "1504679359", null),
        Row(1, "3", "Spain", "1504679459", null),
        Row(1, "4", "India", "1504679659", null),
        Row(1, "5", "France", "1504679559", null),
        Row(1, "6", "Germany", "1504679359", null),
        Row(1, "7", "Italy", "1504679459", null),
        Row(1, "8", "Belgium", "1504679659", null),
        Row(1, "5", "France", "1504679559", "Male"),
        Row(1, "6", "Germany", "1504679359", "Female"),
        Row(1, "7", "Italy", "1504679459", "Female"),
        Row(1, "8", "Belgium", "1504679659", "Male")
      )),
      StructType(expectedSchema)
    )

    val numberOfPartitions = 3
    val intermediatePath = "src/test/resources/custom-input-format/yr=2021/mon=07/dt=19_intermediate"
    val prioritiseIntermediatePath = true

    val df = new FSSparkLoader()
      .addFormat(Avro)
      .lookBack("src/test/resources/custom-input-format/yr=${YR}/mon=${MON}/dt=${DT}", Map("YR" -> "2021", "MON" -> "07", "DT" -> "19"), 3)
      .distributedLoad(Option(numberOfPartitions), intermediatePath, Option(prioritiseIntermediatePath))
      .buildUnsafe(spark)

    val intermediateDf = ParquetSparkLoader().load(intermediatePath).buildUnsafe(spark)

    assert(3,intermediateDf.rdd.getNumPartitions)
    assert(3, df.rdd.getNumPartitions)
    assertDataFrameEquality(expectedDf, df, "DeviceId")
    assertDataFrameEquality(expectedDf, intermediateDf, "DeviceId")
    FileUtils.forceDelete(new File(intermediatePath))
  }
}
