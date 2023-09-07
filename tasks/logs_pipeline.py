try:
    import os , sys , pymysql , findspark , logging
    from flowrunner import BaseFlow, end, start, step

    spark_version = '3.3.2'
    SUBMIT_ARGS = f'--packages ' \
                f'org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version},' \
                f'org.apache.kafka:kafka-clients:2.8.1,' \
                f'org.apache.hadoop:hadoop-aws:3.3.1,org.apache.hadoop:hadoop-client:3.3.0,com.amazonaws:aws-java-sdk-bundle:1.11.563,'\
                f'com.google.guava:guava:30.1.1-jre,org.apache.httpcomponents:httpcore:4.4.14,com.google.inject:guice:4.2.2,'\
                f'com.google.inject.extensions:guice-servlet:4.2.2 '\
                f'pyspark-shell'

    os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
        
    
    findspark.init( '/opt/spark' )
    
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, split , lit , count
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType
    

    print("ok.....")
except Exception as e:
    print("Error : {} ".format(e))



class LogsPipelinePySpark(BaseFlow):
    spark = None
    fail_on_data_loss = "true"

    @start
    @step(next=["bounce_read_stream", "delivered_read_stream"])
    def start_spark_session(self):
        """
        This step is init spark session with and start application.
        connect with master node with name 'pmta_log_pipline' , reserve necessary resources  ,
        enable using sql in streaming and
        connect with S3 and configure S3 as a Storage
        after Connecting to Master we pass to next steps 
        - bounce_read_stream
        - delivered_read_stream
        """
        try:
            self.spark = SparkSession \
                .builder \
                .master(f"spark://{os.environ.get('MASTER_SPARK') or 'MASTER_SPARK'}:7077") \
                .appName("pmta_log_pipeline") \
                .config("spark.executor.memory", "10g") \
                .config('spark.executor.cores', '10') \
                .config('spark.cores.max', '10') \
                .config("spark.sql.adaptive.enabled", "false") \
                .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY") or "AWS_ACCESS_KEY") \
                .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_KEY") or "AWS_SECRET_KEY") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider" )\
                .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')\
                .getOrCreate()
        except Exception as e:
            logging.error("exceptions")
            raise Exception(f"Exception was raised on Step 1 : {e} ")
        
        return self
            
    @step(next=['bounce_transformation'])
    def bounce_read_stream(self):
        """
        Here we listen to kafka streaming topic
        after listening to topic we pass to next step
        - bounce_transformation
        """
        self.data_bounce = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", f'{os.environ.get("KAFKA_HOST") or "KAFKA_HOST"}:{os.environ.get("KAFKA_PORT") or "KAFKA_PORT"}') \
            .option("subscribe", "pmta-bounce") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", self.fail_on_data_loss ) \
            .load()

    @step(next=["union_delivered_bounce"])
    def bounce_transformation(self):
        """     
        Here we transform data and extract necessary data 
        after trasform data we pass to next step
        - union_delivered_bounce
        """
        self.data_bounce = self.data_bounce.withColumn("value", split("value", ",")).selectExpr(
            "value[0] as server_name",
            "cast(value[1] as Date) as date",
            "value[3] as rcpt",
            "split(value[3], '@')[1] as email_domain",
            "split(value[4], '-')[0] as job_id",
            "split(value[4], '-')[1] as sub_id",
            "split(value[4], '-')[2] as is_seed",
            "split(value[4], '-')[3] as num",
            "split(value[4], '-')[4] as user",
            "split(value[5], '-')[0] as domain",
            "split(value[5], '-')[1] as ip",
            "value[6] as bounceCat"    
        ).withColumn("bounce_type", self.funs(F.col("bounceCat"))).select( 
            col("server_name") , col("date"),  col("email_domain") , 
            col("job_id"),  col("user"), col("domain") , col("ip") , lit('bounce').alias("type") , col("bounce_type") 
        ).filter("is_seed == 0")

        return self.data_bounce

    @step(next=["delivered_transformation"])
    def delivered_read_stream(self):
        """
        Here we listen to kafka streaming topic
        after listening to topic we pass to next step
        - delivered_transformation
        """
        self.data_delivered = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", f'{os.environ.get("KAFKA_HOST") or "KAFKA_HOST"}:{os.environ.get("KAFKA_PORT") or "KAFKA_PORT"}') \
            .option("subscribe", "pmta-delivered") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", self.fail_on_data_loss ) \
            .load()


    @step(next=["union_delivered_bounce"])
    def delivered_transformation(self):
        """  
        Here we transform data and extract necessary data 
        after trasform data we pass to next step
        - union_delivered_bounce
        """
        self.data_delivered = self.data_delivered.withColumn("value", split("value", ",")).selectExpr(
            "value[0] as server_name",
            "cast(value[1] as Date) as date",
            "value[3] as rcpt",
            "split(value[3], '@')[1] as email_domain",
            "split(value[4], '-')[0] as job_id",
            "split(value[4], '-')[1] as sub_id",
            "split(value[4], '-')[2] as is_seed",
            "split(value[4], '-')[3] as num",
            "split(value[4], '-')[4] as user",
            "split(value[5], '-')[0] as domain",
            "split(value[5], '-')[1] as ip"
        ).withColumn("bounce_type", lit(None).cast(StringType())).select( 
            col("server_name") , col("date"),  col("email_domain") ,
            col("job_id"),  col("user"), col("domain") , col("ip") , lit('delivered').alias("type") ,  col("bounce_type") 
        ).filter("is_seed == 0")
        

    @step(next=["write_data"])
    def union_delivered_bounce(self):
        """
        Here we merge the two streames together , then applay group by count aggregations 
        after merging and aggregate data we pass to next step
        - write_data
        """
        combined_dstream = self.data_bounce.union(self.data_delivered)
        self.combined_dstream_aggregation = combined_dstream \
            .groupBy(*combined_dstream.columns) \
            .agg( count("*").alias("total_count") )
        
    @step(next=["wait_for_listners"]) 
    def write_data(self):
        """
        Here we store the 2 types , the new final streaming dataframe of aggregated data in database.
        and store row data in S3 storage as cvs file.
        after storing data we pass to next step
        - wait_for_listners
        """
        self.data_bounce.writeStream \
            .outputMode("append") \
            .format("csv") \
            .option("path", "s3a://s3-spark-private-test/verst/bn") \
            .option("checkpointLocation",  "s3a://s3-spark-private-test/checkpoints/bn") \
            .partitionBy("date") \
            .start()

        self.data_delivered.writeStream \
            .outputMode("append") \
            .format("csv") \
            .option("path", "s3a://s3-spark-private-test/verst/dl") \
            .option("checkpointLocation", "s3a://s3-spark-private-test/checkpoints/dl") \
            .partitionBy("date") \
            .start()

        # Write the streaming DataFrame to PostgreSQL using foreachBatch
        self.query = self.combined_dstream_aggregation.writeStream \
            .foreachBatch(self.write_to_mysql) \
            .outputMode("update")\
            .option("checkpointLocation","s3a://s3-spark-private-test/checkpoints/aggr") \
            .start()


    @end
    @step
    def wait_for_listners(self):
        """
        Here we wait for Termination of listenrs. 
        and stop application if still running
        """
        self.query.awaitTermination()
        if self.spark.getActiveSession() :
            self.spark.stop()


    #Process and write the streaming DataFrame to MySQL using foreachBatch
    def write_to_mysql( self , df , _ ):
        pandas_df = df.toPandas()
        colums = " , ".join(list(pandas_df.columns))
        connection = pymysql.connect(
            host=f'{os.environ.get("DATABASE_HOST") or "DATABASE_HOST"}',
            port=int(f'{os.environ.get("DATABASE_PORT") or 3306}'),
            database="streaming_data",
            user=f'{os.environ.get("DATABASE_USER") or "DATABASE_USER"}',
            password=f'{os.environ.get("DATABASE_PASS") or "DATABASE_PASS"}',
            autocommit=True
        )

        cursor = connection.cursor()   
        for _, row in pandas_df.iterrows():
            values = list(row)
            values_string = " , ".join( [ '%s' for _ in values ] )
            sql = f"INSERT INTO logs_table ( { colums } ) VALUES ( {values_string} ) ON DUPLICATE KEY UPDATE total_count = %s"
            # add on duplicate value
            values.append(values[ len(values) - 1 ])
            try :
                cursor.execute(sql, values)
            except pymysql.err.DataError as e :  
                print(f" {str(e)} on {sql} , {values_string} ")
            except:
                print(f" error on {sql} , {values_string} ")
        cursor.close()
        connection.close()

    def funs(self , _ ):
        return F.when(F.col("bounceCat").isin("bad-mailbox", "inactive-mailbox"), "hard") \
            .when(F.col("bounceCat").isin("policy-related", "quota-issues"), "soft") \
            .otherwise("other")