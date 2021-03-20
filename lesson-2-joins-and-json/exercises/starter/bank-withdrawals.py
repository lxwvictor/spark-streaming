from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

# TO-DO: create bank withdrawals kafka message schema StructType including the following JSON elements:
#  {"accountNumber":"703934969","amount":625.8,"dateAndTime":"Sep 29, 2020, 10:06:23 AM","transactionId":1601395583682}
bankWithdrawalsMessageSchema = StructType (
    [
        StructField("accountNumber", StringType()),
        StructField("amount", DoubleType()),
        StructField("dateAndTime", StringType()),
        StructField("transactionId", StringType())
    ]   
)

# TO-DO: create an atm withdrawals kafka message schema StructType including the following JSON elements:
# {"transactionDate":"Sep 29, 2020, 10:06:23 AM","transactionId":1601395583682,"atmLocation":"Thailand"}
atmWithdrawalsMessageSchema = StructType (
    [
        StructField("transactionDate", StringType()),
        StructField("transactionId", StringType()),
        StructField("atmLocation", StringType()),
    ]
)

# TO-DO: create a spark session, with an appropriately named application name
spark = SparkSession.builder.appName("bank-withdrawals").getOrCreate()

#TO-DO: set the log level to WARN
spark.sparkContext.setLogLevel('WARN')

#TO-DO: read the bank-withdrawals kafka topic as a source into a streaming dataframe with the bootstrap server kafka:19092, configuring the stream to read the earliest messages possible                                    
bankWithdrawalsRawStreamingDF = spark                          \
    .readStream                                          \
    .format("kafka")                                     \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","bank-withdrawals")                  \
    .option("startingOffsets","earliest")\
    .load()  

#TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
bankWithdrawalsStreamingDF = bankWithdrawalsRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) json_string")

#TO-DO: using the kafka message StructType, deserialize the JSON from the streaming dataframe 

# TO-DO: create a temporary streaming view called "BankWithdrawals" 
# it can later be queried with spark.sql
bankWithdrawalsStreamingDF.withColumn("json_obj",from_json("json_string",bankWithdrawalsMessageSchema))\
        .select(col('json_obj.*')) \
        .createOrReplaceTempView("BankWithdrawals")

#TO-DO: using spark.sql, select * from BankWithdrawals into a dataframe
bankWithdrawalsSelectStarDF=spark.sql("select * from BankWithdrawals")

#TO-DO: read the atm-withdrawals kafka topic as a source into a streaming dataframe with the bootstrap server kafka:19092, configuring the stream to read the earliest messages possible                                    
customersRawStreamingDF = spark                          \
    .readStream                                          \
    .format("kafka")                                     \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","atm-withdrawals")                  \
    .option("startingOffsets","earliest")\
    .load()      

#TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
atmStreamingDF = customersRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) json_string")

#TO-DO: using the kafka message StructType, deserialize the JSON from the streaming dataframe 

# TO-DO: create a temporary streaming view called "AtmWithdrawals" 
# it can later be queried with spark.sql
atmStreamingDF.withColumn("json_obj",from_json("json_string",atmWithdrawalsMessageSchema))\
        .select(col('json_obj.*')) \
        .createOrReplaceTempView("AtmWithdrawals")

#TO-DO: using spark.sql, select * from AtmWithdrawals into a dataframe
atmSelectStarDF=spark.sql("select * from AtmWithdrawals")

#TO-DO: join the atm withdrawals dataframe with the bank withdrawals dataframe
atmWithdrawalDF = bankWithdrawalsSelectStarDF.join(atmSelectStarDF, "transactionId")

# TO-DO: write the stream to the kafka in a topic called withdrawals-location, and configure it to run indefinitely, the console will not output anything. You will want to attach to the topic using the kafka-console-consumer inside another terminal
# TO-DO: for the "checkpointLocation" option in the writeStream, be sure to use a unique file path to avoid conflicts with other spark scripts
atmWithdrawalDF.selectExpr("cast(transactionId as string) as key", "to_json(struct(*)) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("topic", "withdrawals-location")\
    .option("checkpointLocation","/tmp/kafkacheckpoint")\
    .start()\
    .awaitTermination()
