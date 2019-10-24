# DynamoDB Connector for Structured Streaming 

Implementation of DynamoDB Source Provider in Spark Structured Streaming.
## Developer Setup
Checkout spark-dynamodb-source branch depending upon your Spark version. Use Master branch for the latest Spark version 

###### Spark version 2.4.0
	git clone git@github.com:kolia1985/spark-dynamodb-source.git
	git checkout 2.4.0
	cd spark-dynamodb-source
	sbt build

This will create *target/spark-dynamodb-source_2.11-0.0.2.jar* file which contains the connector code and its dependency jars.

## How to use it

#### Setup DynamoDB
Refer [Amazon Docs](https://docs.aws.amazon.com/cli/latest/reference/dynamodb/create-table.html) for more options

###### Create DynamoDB table with enabled streaming 

	$ aws dynamodb create-table --table-name test --attribute-definitions [your table defenition] --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES
    
###### Add Records in the stream

Add record to the table using aws cli on your custom application.

#### Example Streaming Job

Refering $SPARK_HOME to the Spark installation directory. This library has been developed and tested against **SPARK 2.4.x**. 

###### Open Spark-Shell

	$SPARK_HOME/bin/spark-shell --jars target/spark-dynamodb-source_2.11-0.0.2.jar --packages "com.amazonaws:dynamodb-streams-kinesis-adapter:1.5.0"

###### Subscribe to DynamoDB Source
	// Subscribe the "test" stream
	scala> :paste
	val dynamodb = spark
  		.readStream
  		.format("dynamodb")
    	.option("streamName", "test")
       	.option("regionname", "us-east-1")
        .option("awsAccessKeyId", [ACCESS_KEY])
        .option("awsSecretKey", [SECRET_KEY])
        .option("startingposition", "TRIM_HORIZON")
        .load

###### Check Schema 
	scala> dynamodb.printSchema
	root
 	|-- data: binary (nullable = true)
 	|-- streamName: string (nullable = true)
 	|-- partitionKey: string (nullable = true)
 	|-- sequenceNumber: string (nullable = true)
 	|-- approximateArrivalTimestamp: timestamp (nullable = true)

###### Log data to console 
	// Cast data into string
	scala> :paste
    
        dynamodb
        .select($"data".cast("String")))
        .writeStream
        .format("console")
        .start()
        .awaitTermination()
        
###### Output in Console


	+------------+
	|        data|
	+------------+
	|  ..........|
	+------------+


## DynamoDB Source Configuration 

| Option-Name        | Default-Value           | Description  |
| ------------- |:-------------:| -----:|
| streamName     | - | Name of DynamoDB table to read from |
| endpointUrl     |   -   |   end-point URL for work with Local DynamoDB|
| awsAccessKeyId |    -     |    AWS Credentials for describe, read record operations|   
| awsSecretKey |      -  |    AWS Credentials for describe, read record |
| startingPosition |      LATEST |    Starting Position to fetch data from. Possible values are "LATEST" & "TRIM_HORIZON" |
| describeShardInterval |      1s (1 second) |  Minimum Interval between two DescribeStream API calls to consider resharding  |
| dynamodb.executor.maxFetchTimeInMs |     1000 |  Maximum time spent in executor to fetch record from DynamoDB per Shard |
| dynamodb.executor.maxFetchRecordsPerShard |     100000 |  Maximum Number of records to fetch per shard  |
| dynamodb.executor.maxRecordPerRead |     10000 |  Maximum Number of records to fetch per getRecords API call  |
| dynamodb.client.numRetries |     3 |  Maximum Number of retries for DynamoDB API requests  |
| dynamodb.client.retryIntervalMs |     1000 |  Cool-off period before retrying DynamoDB API  |
