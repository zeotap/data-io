package com.zeotap.source.spark.constructs

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.zeotap.common.types.{INT, OptionalColumn, STRING}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite

import java.io.File

class OptionalColumnOpsTest extends FunSuite with DataFrameSuiteBase {

  val inputAvroPath : String = "src/test/resources/custom-input-format/avro"

  override def beforeAll(): Unit = {
    super.beforeAll()

    val testSchema = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true)
    )

    val sampleDataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559"),
        Row(1,"2","India","1504679359"),
        Row(1,"3","Spain","1504679459"),
        Row(1,"4","India","1504679659")
      )),
      StructType(testSchema)
    )

    sampleDataFrame.write.format("avro").save(inputAvroPath)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    FileUtils.forceDelete(new File(inputAvroPath))
  }

  test("no optional columns provided") {
    val dataFrame = spark.read.format("avro").load(inputAvroPath)
    import com.zeotap.source.spark.constructs.DataFrameOps._
    val actualDataFrame = dataFrame.addOptionalColumns(List())

    assertDataFrameEquals(dataFrame, actualDataFrame)
  }

  test("optional columns provided but already exist in df") {
    val dataFrame = spark.read.format("avro").load(inputAvroPath)
    val optionalColumns = List(
      OptionalColumn("Common_TS", null, STRING),
      OptionalColumn("DeviceId", null, STRING)
    )
    import com.zeotap.source.spark.constructs.DataFrameOps._
    val actualDataFrame = dataFrame.addOptionalColumns(optionalColumns)

    assertDataFrameEquals(dataFrame, actualDataFrame)
  }

  test("optional columns provided that are not present in df") {
    val dataFrame = spark.read.format("avro").load(inputAvroPath)
    val optionalColumns = List(
      OptionalColumn("New_Column", "defaultValue", STRING),
      OptionalColumn("New_Column2", null, STRING),
      OptionalColumn("New_Column3", "10", INT)
    )
    import com.zeotap.source.spark.constructs.DataFrameOps._
    val actualDataFrame = dataFrame.addOptionalColumns(optionalColumns)

    val expectedSchema = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true),
      StructField("New_Column", StringType, false),
      StructField("New_Column2", StringType, true),
      StructField("New_Column3", IntegerType, true)
    )

    val expectedDataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559","defaultValue",null,10),
        Row(1,"2","India","1504679359","defaultValue",null,10),
        Row(1,"3","Spain","1504679459","defaultValue",null,10),
        Row(1,"4","India","1504679659","defaultValue",null,10)
      )),
      StructType(expectedSchema)
    )

    assertDataFrameEquals(expectedDataFrame, actualDataFrame.orderBy(actualDataFrame("DeviceId").asc))
  }

}
