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

import scala.util.{Failure, Random, Success, Try}

import com.amazonaws.auth.{AWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClientBuilder}
import com.amazonaws.services.dynamodbv2.document.{DynamoDB, Item, Table}
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer
import com.amazonaws.services.dynamodbv2.model.{CreateTableRequest, ProvisionedThroughput, KeySchemaElement, AttributeDefinition}

import collection.JavaConverters._
import com.amazonaws.services.dynamodbv2.model.StreamSpecification
import com.amazonaws.services.dynamodbv2.model.StreamViewType


import org.apache.spark.internal.Logging

private[dynamodb] class DynamoDBTestUtils(streamShardCount: Int = 2) extends Logging {

  var server: DynamoDBProxyServer = _
  var ddb: AmazonDynamoDB = _
  var table: Table = _
  val endpointUrl = DynamoDBTestUtils.endpointUrl
  val regionName = DynamoDBTestUtils.regionName

  @volatile
  private var streamCreated = false

  @volatile
  private var _streamName: String = _

  def start {
    System.setProperty("sqlite4java.library.path", System.getProperty("user.dir") + "/native-libs/")

    ddb = DynamoDBEmbedded.create.amazonDynamoDB
    val args = Array("-inMemory")
    server = ServerRunner.createServerFromCommandLineArgs(args)
    server.start()
  } 

  def stop {
    server.stop()
    ddb.shutdown()
  }
 
  val dynamoClientBuilder = AmazonDynamoDBClientBuilder.standard()
    dynamoClientBuilder
      .withCredentials(BasicCredentials(
            DynamoDBTestUtils.getAWSCredentials().getAWSAccessKeyId,
            DynamoDBTestUtils.getAWSCredentials().getAWSSecretKey
          ).provider)
      .withEndpointConfiguration(
        new EndpointConfiguration(endpointUrl, regionName)) // for tests
  val amazonDynamoDBClient = dynamoClientBuilder.build()
  val dynamodb = new DynamoDB(amazonDynamoDBClient)


  def streamName: String = {
    require(streamCreated, "Stream not yet created, call createStream() to create one")
    _streamName
  }

  def createStream(): Unit = {
    require(!streamCreated, "Stream already created")
    _streamName = findNonExistentStreamName()

    // Create a stream. The number of shards determines the provisioned throughput.
    logInfo(s"Creating stream ${_streamName}")

    val createTableRequest = new CreateTableRequest()
      .withTableName(_streamName)
      .withStreamSpecification(new StreamSpecification().withStreamEnabled(true).withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES))
      .withAttributeDefinitions(new AttributeDefinition(DynamoDBTestUtils.IdKey, "N"))
      .withKeySchema(Seq(new KeySchemaElement(DynamoDBTestUtils.IdKey, "HASH")).asJava)
      .withProvisionedThroughput(new ProvisionedThroughput(10L, 10L))

    table = dynamodb.createTable(createTableRequest)

    assert(table.getTableName == _streamName)

    streamCreated = true
    logInfo(s"Created table/stream ${_streamName}")
  }

  

  /**
    * Push data to DynamoDB stream and return a map of
    * shardId -> seq of (data, seq number) pushed to corresponding shard
    */
  def pushData(testData: Array[String], aggregate: Boolean) {
    require(streamCreated, "Stream not yet created, call createStream() to create one")
    testData.foreach(x => table.putItem(new Item().withNumber(DynamoDBTestUtils.IdKey, x.toInt).withString("n", x)))
    testData.foreach(println)
  }

//  /**
//    * Expose a Python friendly API.
//    */
//  def pushData(testData: java.util.List[String]): Unit = {
//    pushData(testData.asScala.toArray, aggregate = false)
//  }

  def deleteStream(): Unit = {
    try {
      if (streamCreated) {
        table.delete()
        table.waitForDelete()
      }
    } catch {
      case e: Exception =>
        logWarning(s"Could not delete stream $streamName")
    }
  }

  private def findNonExistentStreamName(): String = {
    s"DynamoDBTestUtils-${math.abs(Random.nextLong())}"
  }
}

private[dynamodb] object DynamoDBTestUtils {

  val envVarNameForEnablingTests = "ENABLE_KINESIS_SQL_TESTS"
  val endVarNameForEndpoint = "KINESIS_TEST_ENDPOINT_URL"
  val defaultEndpointUrl = "http://localhost:8000"
  val regionName: String = "us-east-1"

  val LocalDynamoDBEndpoint = "http://localhost:8000"
  val TestTableName = "test"
  val IdKey = "id"



  lazy val shouldRunTests = true

  lazy val endpointUrl = {
    val url = sys.env.getOrElse(endVarNameForEndpoint, defaultEndpointUrl)
    // scalastyle:off println
    // Print this so that they are easily visible on the console and not hidden in the log4j logs.
    println(s"Using endpoint URL $url for creating DynamoDB streams for tests.")
    // scalastyle:on println
    url
  }

  def isAWSCredentialsPresent: Boolean = {
    Try { new DefaultAWSCredentialsProviderChain().getCredentials() }.isSuccess
  }

  def getAWSCredentials(): AWSCredentials = {
    assert(shouldRunTests,
      "DynamoDB test not enabled, should not attempt to get AWS credentials")
    Try { new DefaultAWSCredentialsProviderChain().getCredentials() } match {
      case Success(cred) =>
        cred
      case Failure(e) =>
        throw new Exception(
          s"""
             |DynamoDB tests enabled using environment variable $envVarNameForEnablingTests
             |but could not find AWS credentials. Please follow instructions in AWS documentation
             |to set the credentials in your system such that the DefaultAWSCredentialsProviderChain
             |can find the credentials.
           """.stripMargin)
    }
  }
}