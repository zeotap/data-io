package com.zeotap.source.loader.spark.constructs

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.zeotap.source.loader.spark.constructs.LookBackOps.getAllPossiblePaths
import com.zeotap.source.loader.spark.test.helpers.DataFrameUtils.assertDataFrameEquality
import com.zeotap.source.loader.utils.DataPickupUtils
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite

import java.io.File
import java.time.LocalDateTime

class LookBackOpsTest extends FunSuite with DataFrameSuiteBase {

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

  test("getLocalDateTimeFromStandardParametersTest for yr,mon,dt") {
    val expectedDate = LocalDateTime.of(2021, 8, 3, 0, 0)
    val actualDate = LookBackOps.getLocalDateTimeFromStandardParameters(Map("YR" -> "2021", "MON" -> "08", "DT" -> "03"))

    assert(expectedDate == actualDate)
  }

  test("getLocalDateTimeFromStandardParametersTest for yr,mon,dt,hr,min") {
    val expectedDate = LocalDateTime.of(2021, 8, 3, 2, 30)
    val actualDate = LookBackOps.getLocalDateTimeFromStandardParameters(Map("YR" -> "2021", "MON" -> "08", "DT" -> "03", "HR" -> "02", "MIN" -> "30"))

    assert(expectedDate == actualDate)
  }

  test("getStandardParametersFromLocalDateTime") {
    val expectedMap = Map("YR" -> "2021", "MON" -> "08", "DT" -> "03", "HR" -> "02", "MIN" -> "30")
    val actualMap = LookBackOps.getStandardParametersFromLocalDateTime(LocalDateTime.of(2021, 8, 3, 2, 30))

    assert(expectedMap == actualMap)
  }

  test("getAllPossiblePaths") {
    val pathTemplate = "src/test/resources/custom-input-format/yr=${YR}/mon=${MON}/dt=${DT}"
    val parameters = Map("YR" -> "2021", "MON" -> "08", "DT" -> "03")

    val expectedPaths = List("src/test/resources/custom-input-format/yr=2021/mon=08/dt=03",
      "src/test/resources/custom-input-format/yr=2021/mon=08/dt=02")
    val actualPaths = LookBackOps.getAllPossiblePaths(pathTemplate, parameters, 1)

    assert(expectedPaths == actualPaths)
  }

  test("getPathsToPick") {
    val pathTemplate = "src/test/resources/custom-input-format/yr=${YR}/mon=${MON}/dt=${DT}"
    val parameters = Map("YR" -> "2021", "MON" -> "07", "DT" -> "20")
    val possiblePaths = getAllPossiblePaths(pathTemplate, parameters, 3)
    val fileSystem = DataPickupUtils.getFileSystem(pathTemplate)

    val expectedPathsToPick = List(
      "src/test/resources/custom-input-format/yr=2021/mon=07/dt=19",
      "src/test/resources/custom-input-format/yr=2021/mon=07/dt=18",
      "src/test/resources/custom-input-format/yr=2021/mon=07/dt=17"
    )
    val actualPathsToPick = LookBackOps.getPathsToPick(possiblePaths, fileSystem)

    assert(expectedPathsToPick == actualPathsToPick)
  }

  test("getPathsToPick2") {
    val pathTemplate = "src/test/resources/custom-input-format/yr=${YR}/mon=${MON}/dt=${DT}"
    val parameters = Map("YR" -> "2021", "MON" -> "07", "DT" -> "21")
    val possiblePaths = getAllPossiblePaths(pathTemplate, parameters, 3)
    val fileSystem = DataPickupUtils.getFileSystem(pathTemplate)

    val expectedPathsToPick = List(
      "src/test/resources/custom-input-format/yr=2021/mon=07/dt=19",
      "src/test/resources/custom-input-format/yr=2021/mon=07/dt=18"
    )
    val actualPathsToPick = LookBackOps.getPathsToPick(possiblePaths, fileSystem)

    assert(expectedPathsToPick == actualPathsToPick)
  }

  test("lookBackReadTest") {
    val pathTemplate = "src/test/resources/custom-input-format/yr=${YR}/mon=${MON}/dt=${DT}"
    val parameters = Map("YR" -> "2021", "MON" -> "07", "DT" -> "21")

    val expectedDataFrame = spark.read.format("avro").load(List(inputAvroPath1, inputAvroPath2) : _*)
    import com.zeotap.source.loader.spark.constructs.DataFrameReaderOps._
    val actualDataFrame = spark.read.format("avro").lookBack(pathTemplate, parameters, 3)

    assertDataFrameEquality(expectedDataFrame, actualDataFrame, "DeviceId")
  }

}
