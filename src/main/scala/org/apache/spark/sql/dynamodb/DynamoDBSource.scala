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

import java.io._
import java.util.Locale

import com.amazonaws.services.kinesis.model.Record
import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.streaming.{Offset, Source, _}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{SerializableConfiguration, Utils}


 /*
  * A [[Source]] that reads data from DynamoDB using the following design.
  *
  *  - The [[KinesisSourceOffset]] is the custom [[Offset]] defined for this source
  *
  *  - The [[DynamodDBSource]] written to do the following.
  *
  *   - `getOffset()` uses the [[KinesisSourceOffset]] to query the latest
  *      available offsets, which are returned as a [[KinesisSourceOffset]].
  *
  *   - `getBatch()` returns a DF
  *   - The DF returned is based on [[KinesisSourceRDD]]
  */

private[dynamodb] class DynamoDBSource(
                                        sqlContext: SQLContext,
                                        sourceOptions: Map[String, String],
                                        metadataPath: String,
                                        streamName: String,
                                        initialPosition: DynamoDBPosition,
                                        endPointURL: String,
                                        regionName: String,
                                        credsProvider: SparkAWSCredentials
    )
  extends Source with Serializable with Logging {

  import DynamoDBSource._

  private def sc: SparkContext = {
    sqlContext.sparkContext
  }

  private def dynamodbReader: DynamoDBReader = {
    new DynamoDBReader(sourceOptions, streamName, credsProvider, endPointURL, regionName)
  }

  private var currentShardOffsets: Option[ShardOffsets] = None

  private val minBatchesToRetain = sqlContext.sparkSession.sessionState.conf.minBatchesToRetain
  require(minBatchesToRetain > 0, "minBatchesToRetain has to be positive")

  private val describeShardInterval: Long = {
    Utils.timeStringAsMs(sourceOptions.getOrElse("describeShardInterval", "1s"))
  }

  require(describeShardInterval >= 0, "describeShardInterval cannot be less than 0 sec")

  private var latestDescribeShardTimestamp: Long = -1L

  private def metadataCommitter: MetadataCommitter[ShardInfo] = {
    metaDataCommitterType.toLowerCase(Locale.ROOT) match {
      case "hdfs" =>
        new HDFSMetadataCommitter[ ShardInfo ](metaDataCommitterPath, hadoopConf(sqlContext))
      case "dbfs" => {
          if (metaDataCommitterPath.startsWith("dbfs")) {
            val metaDatapath = metaDataCommitterPath.replace("dbfs:/", "file:///dbfs")
            new HDFSMetadataCommitter[ ShardInfo ](metaDatapath, hadoopConf(sqlContext))
            } else {
              throw new IllegalArgumentException("Metadata Committer Path should contain dbfs schema")
            }
        }
      case _ => throw new IllegalArgumentException("only HDFS and DBFS is supported")
    }
  }

  private def metaDataCommitterType: String = {
    sourceOptions.getOrElse("executor.metadata.committer", "hdfs").toString
  }

  private def metaDataCommitterPath: String = {
    sourceOptions.getOrElse("executor.metadata.path", metadataPath).toString
  }

  /** Returns the shards position to start reading data from */
  override def getOffset: Option[Offset] = synchronized {
    val defaultOffset = new ShardOffsets(-1L, streamName)
    val prevBatchId = currentShardOffsets.getOrElse(defaultOffset).batchId
    val prevShardsInfo = prevBatchShardInfo(prevBatchId)

    val latestShardInfo: Array[ShardInfo] = {
      if (latestDescribeShardTimestamp == -1 ||
        ((latestDescribeShardTimestamp + describeShardInterval) < System.currentTimeMillis())) {
        val latestShards = dynamodbReader.getShards()
        if (latestShards.nonEmpty) {
          latestDescribeShardTimestamp = System.currentTimeMillis()
          ShardSyncer.getLatestShardInfo(latestShards, prevShardsInfo, initialPosition)
        } else {
          Seq.empty[ShardInfo]
        }
      }
      else {
        prevShardsInfo
      }
    }.toArray

    // update currentShardOffsets only when latestShardInfo is not empty
    // else use last batch's ShardOffsets.
    // Since there wont be any change in offset, no new batch will be triggered
    if (latestShardInfo.nonEmpty) {
      currentShardOffsets = Some(new ShardOffsets(prevBatchId + 1, streamName, latestShardInfo))
    }

    currentShardOffsets match {
      case None => None
      case Some(cso) => Some(DynamoDBSourceOffset(cso))
    }
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    logInfo(s"End Offset is ${end.toString}")
    val currBatchShardOffset = DynamoDBSourceOffset.getShardOffsets(end)
    val currBatchId = currBatchShardOffset.batchId
    var prevBatchId: Long = start match {
      case Some(prevBatchStartOffset) =>
        DynamoDBSourceOffset.getShardOffsets(prevBatchStartOffset).batchId
      case None => -1.toLong
    }
    assert(prevBatchId <= currBatchId)

    val shardInfos = {
      // filter out those shardInfos for which ShardIterator is shard_end
      currBatchShardOffset.shardInfo.filter {
        s: (ShardInfo) => !(s.iteratorType.contains(new ShardEnd().iteratorType))
      }.sortBy(_.shardId.toString)
    }
    logInfo(s"Processing ${shardInfos.length} shards from ${shardInfos}")

    // Create an RDD that reads from Kinesis
    val dynamodbSourceRDD = new DynamoDBSourceRDD(
      sc,
      sourceOptions,
      streamName,
      currBatchId,
      shardInfos,
      credsProvider,
      endPointURL,
      regionName,
      hadoopConf(sqlContext),
      metadataPath)

    val rdd = dynamodbSourceRDD.map { r: Record =>
      InternalRow(
        r.getData.array(),
        UTF8String.fromString(streamName),
        UTF8String.fromString(r.getPartitionKey),
        UTF8String.fromString(r.getSequenceNumber),
        DateTimeUtils.fromJavaTimestamp(
          new java.sql.Timestamp(r.getApproximateArrivalTimestamp.getTime))
      )
    }

    // On recovery, getBatch will get called before getOffset
    if (currentShardOffsets.isEmpty) {
      currentShardOffsets = Some(currBatchShardOffset)
    }

    logInfo("GetBatch generating RDD of offset range: " +
      shardInfos.mkString(", "))

    sqlContext.internalCreateDataFrame(rdd, schema, isStreaming = true)

  }

  override def schema: StructType = DynamoDBReader.dynamodbSchema

  /** Stop this source and free any resources it has allocated. */
  override def stop(): Unit = synchronized {
    dynamodbReader.close()
  }

  override def commit(end: Offset): Unit = {
    val defaultOffset = new ShardOffsets(-1L, streamName)
    val currBatchId = currentShardOffsets.getOrElse(defaultOffset).batchId
    val thresholdBatchId = currBatchId - minBatchesToRetain
    if (thresholdBatchId >= 0) {
      logInfo(s"Purging Committed Entries. ThresholdBatchId = ${thresholdBatchId}")
      metadataCommitter.purge(thresholdBatchId)
    }
  }

  override def toString(): String = s"DynamoDBSource[$streamName]"

  private def prevBatchShardInfo(batchId: Long): Seq[ShardInfo] = {
    val shardInfo = if (batchId < 0) {
      logInfo(s"This is the first batch. Returning Empty sequence")
      Seq.empty[ShardInfo]
    } else {
      logDebug(s"BatchId of previously executed batch is $batchId")
      val prevShardinfo = metadataCommitter.get(batchId)
      if (prevShardinfo == null || prevShardinfo.isEmpty) {
        throw new IllegalStateException(s"Unable to fetch " +
          s"committed metadata from previous batch. Some data may have been missed")
      }
      prevShardinfo
    }
    logDebug(s"Shard Info is ${shardInfo.mkString(", ")}")
    shardInfo
  }

}

object DynamoDBSource {

  val VERSION = 1

  private var _hadoopConf: SerializableConfiguration = null

  def hadoopConf(sqlContext: SQLContext): SerializableConfiguration = {
    if (_hadoopConf == null) {
      val conf: Configuration = sqlContext.sparkSession.sessionState.newHadoopConf()
      _hadoopConf = new SerializableConfiguration(conf)
    }
    _hadoopConf
  }

}
