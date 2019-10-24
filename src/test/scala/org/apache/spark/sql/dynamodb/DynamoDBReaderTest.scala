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

import org.scalatest.PrivateMethodTester
import scala.util.Try
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.test.SharedSQLContext

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.document.Table

import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer



class DynamoDBReaderSuite extends SharedSQLContext with PrivateMethodTester with BeforeAndAfter{

  val LocalDynamoDBEndpoint = "http://localhost:8000"
  var server: DynamoDBProxyServer = _
  var ddb: AmazonDynamoDB = _
  var table: Table = _
  val TestTableName = "test"
  val IdKey = "id"

  protected var testUtils: DynamoDBTestUtils = _


  before {
    testUtils = new DynamoDBTestUtils
    testUtils.start
    testUtils.createStream
  }

  after {
    if (testUtils != null) {
      testUtils.stop
      testUtils = null
    }
  }

  test("Should succeed for valid Credentials") {
    Try {
        val dynamoDBReader =
          new DynamoDBReader(
            Map.empty[String, String],
            testUtils.streamName,
            BasicCredentials(
             "AWSAccessKeyId",
             "AWSSecretKey"
           ),
            LocalDynamoDBEndpoint,
            ""
          )
        dynamoDBReader.getShards()
    }.isSuccess
  }
}
