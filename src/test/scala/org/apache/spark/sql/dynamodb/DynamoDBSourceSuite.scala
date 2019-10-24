/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.dynamodb

import java.util.Locale

import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.streaming.{ProcessingTime, StreamTest}
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{StructType, DataType}
import org.apache.spark.sql.functions._


abstract class DynamoDBSourceTest()
  extends StreamTest with SharedSQLContext {

  protected var testUtils: DynamoDBTestUtils = _

  override val streamingTimeout = 30.seconds

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new DynamoDBTestUtils
    testUtils.start
    testUtils.createStream
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.stop
      testUtils = null
      super.afterAll()
    }
  }

  import DynamoDBTestUtils._

  /** Run the test if environment variable is set or ignore the test */
  def testIfEnabled(testName: String)(testBody: => Unit) {
    if (shouldRunTests) {
      test(testName)(testBody)
    } else {
      ignore(s"$testName [enable by setting env var $envVarNameForEnablingTests=1]")(testBody)
    }
  }

  /** Run the give body of code only if Kinesis tests are enabled */
  def runIfTestsEnabled(message: String)(body: => Unit): Unit = {
    if (shouldRunTests) {
      body
    } else {
      ignore(s"$message [enable by setting env var $envVarNameForEnablingTests=1]")(())
    }
  }

}


class DynamoDBSourceOptionsSuite extends StreamTest with SharedSQLContext {

  test("bad source options") {
    def testBadOptions(options: (String, String)*)(expectedMsgs: String*): Unit = {
      val ex = intercept[ IllegalArgumentException ] {
        val reader = spark.readStream.format("dynamodb")
        options.foreach { case (k, v) => reader.option(k, v) }
        reader.load()
      }
      expectedMsgs.foreach { m =>
        assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(m.toLowerCase(Locale.ROOT)))
      }
    }

    testBadOptions()("Stream name is a required field")
    testBadOptions("streamname" -> "")("Stream name is a required field")
  }
}


class DynamoDBSourceSuite extends DynamoDBSourceTest {

  import testImplicits._

  test("output data should have valid Kinesis Schema ") {
    val now = System.currentTimeMillis()
    testUtils.pushData(Array(1).map(_.toString), false)
    Thread.sleep(1000.toLong)

    val reader = spark
      .readStream
      .format("dynamodb")
      .option("streamName", testUtils.streamName)
      .option("endpointUrl", testUtils.endpointUrl)
      .option("AWSAccessKeyId", DynamoDBTestUtils.getAWSCredentials().getAWSAccessKeyId)
      .option("AWSSecretKey", DynamoDBTestUtils.getAWSCredentials().getAWSSecretKey)
      .option("startingposition", "TRIM_HORIZON")
      //.option("startingposition", "latest")

    val dynamodbStream = reader.load()
    assert (dynamodbStream.schema == DynamoDBReader.dynamodbSchema)
    val result = dynamodbStream.selectExpr("CAST(data AS STRING)", "streamName",
      "partitionKey", "sequenceNumber", "CAST(approximateArrivalTimestamp AS TIMESTAMP)")
      .as[(String, String, String, String, Long)]

    val query = result.writeStream
      .format("memory")
      .queryName("schematest")
      .start()

    query.awaitTermination(streamingTimeout.toMillis)

    // sleep for 2s for atleast one trigger completion
    Thread.sleep(2000.toLong)

    val rows = spark.table("schematest").collect()
    assert(rows.length === 1, s"Unexpected results: ${rows.toList}")
    val row = rows(0)


    assert(row.getAs[String]("streamName") === testUtils.streamName, s"Unexpected results: $row")
    assert(row.getAs[String]("sequenceNumber") === "000000000000000000001", s"Unexpected results: $row")
    query.stop()
  }

  test("check DBFS metadata committer ") {
    val ex = intercept[org.apache.spark.sql.streaming.StreamingQueryException] {
      val now = System.currentTimeMillis()
      testUtils.pushData(Array(1).map(_.toString), false)
      Thread.sleep(1000.toLong)

      val reader = spark
        .readStream
        .format("dynamodb")
        .option("streamName", testUtils.streamName)
        .option("endpointUrl", testUtils.endpointUrl)
        .option("AWSAccessKeyId", DynamoDBTestUtils.getAWSCredentials().getAWSAccessKeyId)
        .option("AWSSecretKey", DynamoDBTestUtils.getAWSCredentials().getAWSSecretKey)
        .option("startingposition", "TRIM_HORIZON")
        .option("dynamodb.executor.metadata.committer", "dbfs")
        .option("dynamodb.executor.metadata.path", "dbfs://tmp/shard-info/0/")
        //.option("startingposition", "latest")

      val dynamodbStream = reader.load()
      val result = dynamodbStream.selectExpr("CAST(data AS STRING)", "streamName",
        "partitionKey", "sequenceNumber", "CAST(approximateArrivalTimestamp AS TIMESTAMP)")
        .as[(String, String, String, String, Long)]

      val query = result.writeStream
        .format("memory")
        .queryName("schematest")
        .start()

    query.awaitTermination(streamingTimeout.toMillis)
    }
    // We do not have possibility to configure path of mount dbfs on Databricks cluster
    // so we can only catch exception and check if path right in exception message
    assert(ex.message contains "mkdir of file:/dbfs/tmp/shard-info/0/shard-commit failed")
  }

  // Looks like local dynamodb does not support 'latest' type of shard iterator
  ignore("Starting position is latest by default") {
    testUtils.pushData(Array("0"), false)
    // sleep for 1 s to avoid any concurrency issues
    Thread.sleep(2000.toLong)
    val clock = new StreamManualClock

    val waitUntilBatchProcessed = AssertOnQuery { q =>
      //Thread.sleep(5000.toLong)
      eventually(Timeout(streamingTimeout)) {
        if (!q.exception.isDefined) {
          assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
        }
      }
      if (q.exception.isDefined) {
        throw q.exception.get
      }
      true
    }

    val reader = spark
      .readStream
      .format("dynamodb")
      .option("streamName", testUtils.streamName)
      .option("endpointUrl", testUtils.endpointUrl)
      .option("AWSAccessKeyId", DynamoDBTestUtils.getAWSCredentials().getAWSAccessKeyId)
      .option("AWSSecretKey", DynamoDBTestUtils.getAWSCredentials().getAWSSecretKey)
      //.option("startingposition", "TRIM_HORIZON")
      .option("startingposition", "latest")
      .option("describeShardInterval", "0")
    
    val jsonSchema = """{"type":"struct","fields":[{"name":"awsRegion","type":"string","nullable":true,"metadata":{}},{"name":"dynamodb","type":{"type":"struct","fields":[{"name":"ApproximateCreationDateTime","type":"long","nullable":true,"metadata":{}},{"name":"Keys","type":{"type":"struct","fields":[{"name":"id","type":{"type":"struct","fields":[{"name":"N","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},{"name":"NewImage","type":{"type":"struct","fields":[{"name":"id","type":{"type":"struct","fields":[{"name":"N","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},{"name":"n","type":{"type":"struct","fields":[{"name":"S","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},{"name":"SequenceNumber","type":"string","nullable":true,"metadata":{}},{"name":"SizeBytes","type":"long","nullable":true,"metadata":{}},{"name":"StreamViewType","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},{"name":"eventID","type":"string","nullable":true,"metadata":{}},{"name":"eventName","type":"string","nullable":true,"metadata":{}},{"name":"eventSource","type":"string","nullable":true,"metadata":{}},{"name":"eventVersion","type":"string","nullable":true,"metadata":{}}]}"""
    val schema = DataType.fromJson(jsonSchema).asInstanceOf[StructType]

    val dynamodbStream = reader.load()
      .select(from_json(col("data").cast("string"), schema).alias("event"))
      .select("event.dynamodb.NewImage.n.S")
      .as[String]
 
    val result = dynamodbStream.map(_.toInt)
    val testData = 1 to 5
    val testData1 = 10 to 15
    testStream(result)(
      StartStream(ProcessingTime(100), clock),
      waitUntilBatchProcessed,
      AssertOnQuery { query =>
        testUtils.pushData(testData.map(_.toString).toArray, false)
        //Thread.sleep(1000.toLong)
        true
      },
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      AssertOnQuery { query =>
        testUtils.pushData(testData1.map(_.toString).toArray, false)
        //Thread.sleep(1000.toLong)
        true
      },
      AdvanceManualClock(5000),
      AdvanceManualClock(5000),
      waitUntilBatchProcessed,
      CheckAnswer(1, 2, 3, 4, 5)  // should not have 0
    )
  }

}