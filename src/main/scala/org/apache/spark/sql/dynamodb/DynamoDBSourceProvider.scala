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

import java.util.{Locale, Optional}

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

/*
 * The provider class for the [[DynamodDBSource]]. This provider is designed such that it throws
 * IllegalArgumentException when the DynamoDB Dataset is created, so that it can catch
 * missing options even before the query is started.
 */

private[dynamodb] class DynamoDBSourceProvider extends DataSourceRegister
  with StreamSourceProvider
  with Logging {

  import DynamoDBSourceProvider._

  override def shortName(): String = "dynamodb"

  /*
   *  Returns the name and schema of the source. In addition, it also verifies whether the options
   * are correct and sufficient to create the [[DynamoDBSource]] when the query is started.
   */

  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }
    validateStreamOptions(caseInsensitiveParams)
    require(schema.isEmpty, "DynamoDB source has a fixed schema and cannot be set with a custom one")
    (shortName(), DynamoDBReader.dynamodbSchema)
  }

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {

    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }

    validateStreamOptions(caseInsensitiveParams)

    val specifiedDynamoDBParams =
      parameters
        .keySet
        .filter(_.toLowerCase(Locale.ROOT).startsWith(PREFIX_PARAM_NAME))
        .map { k => k.drop(PREFIX_PARAM_NAME.size).toString -> parameters(k) }
        .toMap

    val streamName = caseInsensitiveParams.get(STREAM_NAME_KEY).get

    val awsAccessKeyId = caseInsensitiveParams.get(AWS_ACCESS_KEY_ID).getOrElse("")
    val awsSecretKey = caseInsensitiveParams.get(AWS_SECRET_KEY).getOrElse("")
    val awsStsRoleArn = caseInsensitiveParams.get(AWS_STS_ROLE_ARN).getOrElse("")
    val awsStsSessionName = caseInsensitiveParams.get(AWS_STS_SESSION_NAME).getOrElse("")

    val regionName = caseInsensitiveParams.get(REGION_NAME_KEY)
      .getOrElse(DEFAULT_DYNAMODB_REGION_NAME)
    val endPointURL = caseInsensitiveParams.get(END_POINT_URL)
      .getOrElse(DEFAULT_DYNAMODB_ENDPOINT_URL)

    val initialPosition: DynamoDBPosition = getDynamoDBPosition(caseInsensitiveParams)

    val dynamodbCredsProvider = if (awsAccessKeyId.length > 0) {
      BasicCredentials(awsAccessKeyId, awsSecretKey)
    } else if (awsStsRoleArn.length > 0) {
      STSCredentials(awsStsRoleArn, awsStsSessionName)
    } else {
      InstanceProfileCredentials
    }

    new DynamoDBSource(
      sqlContext, specifiedDynamoDBParams, metadataPath,
      streamName, initialPosition, endPointURL, regionName, dynamodbCredsProvider)
  }

  private def validateStreamOptions(caseInsensitiveParams: Map[String, String]) = {
    if (!caseInsensitiveParams.contains(STREAM_NAME_KEY) ||
      caseInsensitiveParams.get(STREAM_NAME_KEY).get.isEmpty) {
      throw new IllegalArgumentException(
        "Stream name is a required field")
    }
  }

}

private[dynamodb] object DynamoDBSourceProvider extends Logging {

  private[dynamodb] val STREAM_NAME_KEY = "streamname"
  private[dynamodb] val END_POINT_URL = "endpointurl"
  private[dynamodb] val REGION_NAME_KEY = "regionname"
  private[dynamodb] val AWS_ACCESS_KEY_ID = "awsaccesskeyid"
  private[dynamodb] val AWS_SECRET_KEY = "awssecretkey"
  private[dynamodb] val AWS_STS_ROLE_ARN = "awsstsrolearn"
  private[dynamodb] val AWS_STS_SESSION_NAME = "awsstssessionname"
  private[dynamodb] val STARTING_POSITION_KEY = "startingposition"
  private[dynamodb] val PREFIX_PARAM_NAME = "dynamodb."

  private[dynamodb] def getDynamoDBPosition(
      params: Map[String, String]): DynamoDBPosition = {
    // TODO Support custom shards positions
    val CURRENT_TIMESTAMP = System.currentTimeMillis
    params.get(STARTING_POSITION_KEY).map(_.trim) match {
      case Some(position) if position.toLowerCase(Locale.ROOT) == "latest" =>
        new Latest //AtTimeStamp(CURRENT_TIMESTAMP)
      case Some(position) if position.toLowerCase(Locale.ROOT) == "trim_horizon" =>
        new TrimHorizon
      case None => new Latest
    }
  }

  private[dynamodb] val DEFAULT_DYNAMODB_ENDPOINT_URL: String = ""

  private[dynamodb] val DEFAULT_DYNAMODB_REGION_NAME: String = "us-east-1"

}



