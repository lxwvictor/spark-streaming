from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

redisMessageSchema = StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("expiredType", StringType()),
        StructField("expiredValue",StringType()),
        StructField("existType", StringType()),
        StructField("ch", StringType()),
        StructField("incr",BooleanType()),
        StructField("zSetEntries", ArrayType( \
            StructType([
                StructField("element", StringType()),\
                StructField("score", StringType())   \
            ]))                                      \
        )

    ]
)

# TO-DO: create a StructType for the Customer schema for the following fields:
# {"customerName":"Frank Aristotle","email":"Frank.Aristotle@test.com","phone":"7015551212","birthDay":"1948-01-01","accountNumber":"750271955","location":"Jordan"}
customerJSONSchema = StructType (
    [
        StructField("customerName",StringType()),
        StructField("email",StringType()),
        StructField("phone",StringType()),
        StructField("birthDay",StringType()),
        StructField("accountNumber",StringType()),
        StructField("location",StringType())        
    ]
)

# TO-DO: create a spark session, with an appropriately named application name
spark = SparkSession.builder.appName("customer-record").getOrCreate()

#TO-DO: set the log level to WARN
spark.sparkContext.setLogLevel('WARN')

#TO-DO: read the redis-server kafka topic as a source into a streaming dataframe with the bootstrap server kafka:19092, configuring the stream to read the earliest messages possible                                    
redisServerRawStreamingDF = spark                          \
    .readStream                                          \
    .format("kafka")                                     \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","redis-server")                  \
    .option("startingOffsets","earliest")\
    .load()  

#TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
redisServerStreamingDF = redisServerRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

#TO-DO: using the redisMessageSchema StructType, deserialize the JSON from the streaming dataframe 

# TO-DO: create a temporary streaming view called "RedisData" based on the streaming dataframe
# it can later be queried with spark.sql
redisServerStreamingDF.withColumn("value",from_json("value",redisMessageSchema))\
        .select(col('value.*')) \
        .createOrReplaceTempView("RedisData")

#TO-DO: using spark.sql, select key, zSetEntries[0].element as customer from RedisData
zSetEntriesEncodedStreamingDF=spark.sql("select key, zSetEntries[0].element as customer from RedisData")

#TO-DO: from the dataframe use the unbase64 function to select a column called customer with the base64 decoded JSON, and cast it to a string
zSetDecodedEntriesStreamingDF= zSetEntriesEncodedStreamingDF.withColumn("customer",  unbase64(zSetEntriesEncodedStreamingDF.customer).cast("string"))

#TO-DO: using the customer StructType, deserialize the JSON from the streaming dataframe, selecting column customer.* as a temporary view called Customer 
zSetDecodedEntriesStreamingDF\
    .withColumn("customer", from_json("customer", customerJSONSchema))\
    .select(col('customer.*'))\
    .createOrReplaceTempView("Customer")

#TO-DO: using spark.sql select accountNumber, location, birthDay from Customer where birthDay is not null
customerStreamingDF = spark.sql("select accountNumber, location, birthDay from Customer where birthDay is not null")

#TO-DO: select the account number, location, and birth year (using split)
relevantCustomerFieldsStreamingDF = customerStreamingDF.select('accountNumber','location',split(customerStreamingDF.birthDay,"-").getItem(0).alias("birthYear"))

# TO-DO: write the stream in JSON format to a kafka topic called customer-attributes, and configure it to run indefinitely, the console output will not show any output. 
# TO-DO: for the "checkpointLocation" option in the writeStream, be sure to use a unique file path to avoid conflicts with other spark scripts
# You will need to type 'docker exec -it nd029-c2-apache-spark-and-spark-streaming_kafka_1 kafka-console-consumer --bootstrap-server localhost:19092 --topic customer-attributes' to see the JSON data
#
# The data will look like this: {"accountNumber":"288485115","location":"Brazil","birthYear":"1938"}
relevantCustomerFieldsStreamingDF.selectExpr("CAST(accountNumber AS STRING) AS key", "to_json(struct(*)) AS value")\
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("topic", "customer-attributes")\
    .option("checkpointLocation","/tmp/kafkacheckpoint")\
    .start()\
    .awaitTermination()

