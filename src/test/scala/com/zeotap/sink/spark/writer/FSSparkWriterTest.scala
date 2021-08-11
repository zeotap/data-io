package com.zeotap.sink.spark.writer

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.zeotap.common.types._
import com.zeotap.test.helpers.DataFrameUtils.{assertDataFrameEquality, safeColumnUnion}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite

import java.io.File

class FSSparkWriterTest extends FunSuite with DataFrameSuiteBase {

  val avroPath : String = "src/test/resources/custom-input-format/yr=2021/mon=08/dt=05"

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  test("basicWriteTest") {
    val schema = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true)
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559"),
        Row(1,"2","India","1504679359"),
        Row(1,"3","Spain","1504679459"),
        Row(1,"4","India","1504679659")
      )),
      StructType(schema)
    )

    new FSSparkWriter()
      .addFormat(AVRO)
      .addSaveMode(OVERWRITE)
      .save(avroPath)
      .buildUnsafe(df)

    val savedDf = spark.read.format("avro").load(avroPath)
    assertDataFrameEquality(df, savedDf, "DeviceId")
    FileUtils.forceDelete(new File(avroPath))
  }

  test("errorIfExistsTest") {
    val schema = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true)
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559"),
        Row(1,"2","India","1504679359"),
        Row(1,"3","Spain","1504679459"),
        Row(1,"4","India","1504679659")
      )),
      StructType(schema)
    )

    df.write.format("avro").save(avroPath)

    val eitherExceptionOrSave = new FSSparkWriter()
      .addFormat(AVRO)
      .addSaveMode(ERROR_IF_EXISTS)
      .save(avroPath)
      .buildSafe(df)

    assert(eitherExceptionOrSave.isLeft)
    assert(!eitherExceptionOrSave.isRight)
    assert(eitherExceptionOrSave.left.get.contains("already exists"))
    FileUtils.forceDelete(new File(avroPath))
  }

  test("overwriteTest") {
    val schema1 = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true)
    )

    val df1 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559"),
        Row(1,"2","India","1504679359"),
        Row(1,"3","Spain","1504679459"),
        Row(1,"4","India","1504679659")
      )),
      StructType(schema1)
    )

    df1.write.format("avro").save(avroPath)

    val schema2 = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true),
      StructField("DeviceId", StringType, true)
    )

    val df2 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"France","1504679559","5"),
        Row(1,"Germany","1504679359","6"),
        Row(1,"Italy","1504679459","7"),
        Row(1,"Belgium","1504679659","8")
      )),
      StructType(schema2)
    )

    val eitherExceptionOrSave = new FSSparkWriter()
      .addFormat(AVRO)
      .addSaveMode(OVERWRITE)
      .save(avroPath)
      .buildSafe(df2)

    assert(!eitherExceptionOrSave.isLeft)
    assert(eitherExceptionOrSave.isRight)

    val actualDf = spark.read.format("avro").load(avroPath)

    assertDataFrameEquality(df2, actualDf, "DeviceId")
    FileUtils.forceDelete(new File(avroPath))
  }

  test("ignoreTest") {
    val schema1 = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true)
    )

    val df1 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559"),
        Row(1,"2","India","1504679359"),
        Row(1,"3","Spain","1504679459"),
        Row(1,"4","India","1504679659")
      )),
      StructType(schema1)
    )

    df1.write.format("avro").save(avroPath)

    val schema2 = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true),
      StructField("DeviceId", StringType, true)
    )

    val df2 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"France","1504679559","5"),
        Row(1,"Germany","1504679359","6"),
        Row(1,"Italy","1504679459","7"),
        Row(1,"Belgium","1504679659","8")
      )),
      StructType(schema2)
    )

    val eitherExceptionOrSave = new FSSparkWriter()
      .addFormat(AVRO)
      .addSaveMode(IGNORE)
      .save(avroPath)
      .buildSafe(df2)

    assert(!eitherExceptionOrSave.isLeft)
    assert(eitherExceptionOrSave.isRight)

    val actualDf = spark.read.format("avro").load(avroPath)

    assertDataFrameEquality(df1, actualDf, "DeviceId")
    FileUtils.forceDelete(new File(avroPath))
  }

  test("appendTest") {
    val schema1 = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true)
    )

    val df1 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559"),
        Row(1,"2","India","1504679359"),
        Row(1,"3","Spain","1504679459"),
        Row(1,"4","India","1504679659")
      )),
      StructType(schema1)
    )

    df1.write.format("avro").save(avroPath)

    val schema2 = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true),
      StructField("DeviceId", StringType, true)
    )

    val df2 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"France","1504679559","5"),
        Row(1,"Germany","1504679359","6"),
        Row(1,"Italy","1504679459","7"),
        Row(1,"Belgium","1504679659","8")
      )),
      StructType(schema2)
    )

    val eitherExceptionOrSave = new FSSparkWriter()
      .addFormat(AVRO)
      .addSaveMode(APPEND)
      .save(avroPath)
      .buildSafe(df2)

    assert(!eitherExceptionOrSave.isLeft)
    assert(eitherExceptionOrSave.isRight)

    val actualDf = spark.read.format("avro").load(avroPath)
    assertDataFrameEquality(safeColumnUnion(df1, df2), actualDf, "DeviceId")
    FileUtils.forceDelete(new File(avroPath))
  }

  test("partitionByTest") {
    val schema = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true)
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559"),
        Row(1,"2","India","1504679359"),
        Row(1,"3","Spain","1504679459"),
        Row(1,"4","India","1504679659")
      )),
      StructType(schema)
    )

    val eitherExceptionOrSave = new FSSparkWriter()
      .addFormat(AVRO)
      .partitionBy(List("Demographic_Country"))
      .save(avroPath)
      .buildSafe(df)

    assert(!eitherExceptionOrSave.isLeft)
    assert(eitherExceptionOrSave.isRight)

    val partitionedDf1 = spark.read.format("avro").load(avroPath + "/Demographic_Country=India")
    val partitionedDf2 = spark.read.format("avro").load(avroPath + "/Demographic_Country=Spain")

    assert(partitionedDf1.count() == 3)
    assert(partitionedDf2.count() == 1)
    assertDataFrameEquality(df.drop("Demographic_Country"), safeColumnUnion(partitionedDf1, partitionedDf2), "DeviceId")
    FileUtils.forceDelete(new File(avroPath))
  }

}
