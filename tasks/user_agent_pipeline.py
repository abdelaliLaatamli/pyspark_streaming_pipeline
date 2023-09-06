try:
    import os , sys , pymysql , findspark , logging
    from flowrunner import BaseFlow, end, start, step
    from user_agents import parse

    spark_version = '3.3.2'
    SUBMIT_ARGS = f'--packages ' \
                f'org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version},' \
                f'org.apache.kafka:kafka-clients:2.8.1,' \
                f'mysql:mysql-connector-java:8.0.32,'\
                f'org.apache.hadoop:hadoop-aws:3.3.1,org.apache.hadoop:hadoop-client:3.3.0,com.amazonaws:aws-java-sdk-bundle:1.11.563,'\
                f'com.google.guava:guava:30.1.1-jre,org.apache.httpcomponents:httpcore:4.4.14,com.google.inject:guice:4.2.2,'\
                f'com.google.inject.extensions:guice-servlet:4.2.2 '\
                f'pyspark-shell'

    os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
        
    
    findspark.init( '/opt/spark' )
    
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, split , lit , count , udf
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructField , StructType , StringType , IntegerType
    

    print("ok.....")
except Exception as e:
    print("Error : {} ".format(e))




class UserAgentPySpark(BaseFlow):
    spark = None
    fail_on_data_loss = "true"


    @start
    @step(next=["actions_read_stream" , "esp_domains_read_as_stream"])
    def start_spark_session(self):
        """
        This step is init spark session with and start application.
        connect with master node with name 'user_agent_pipline' , reserve necessary resources and
        enable using sql in streaming 
        after Connecting to Master we pass to next steps 
        - actions_read_stream
        - esp_domains_read_as_stream
        """
        try:
            self.spark = SparkSession \
                .builder \
                .master(f"spark://{os.environ.get('MASTER_SPARK') or 'MASTER_SPARK'}:7077") \
                .appName("user_agent_pipline") \
                .config("spark.executor.memory", "10g") \
                .config('spark.executor.cores', '10') \
                .config('spark.cores.max', '10') \
                .config("spark.sql.adaptive.enabled", "false") \
                .getOrCreate()
        except Exception as e:
            logging.error("exceptions")
            raise Exception(f"Exception was raised on Step 1 : {e} ")
        
        return self
    
    @step(next=['join_actions_with_esp_domains_stream'])
    def actions_read_stream(self):
        """
        Here we listen to kafka streaming topic then 
        we transform data and generate new columns with details
        after listening to topic we pass to next step
        - join_actions_with_esp_domains_stream
        """
        self.data_opens = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", f'{os.environ.get("KAFKA_HOST") or "KAFKA_HOST"}:{os.environ.get("KAFKA_PORT") or "KAFKA_PORT"}') \
            .option("subscribe", "click_actions") \
            .option("startingOffsets", "earliest") \
            .load()

        def user_agent_infos(text):
        # Perform your transformation logic here
            transformed_text = text.replace("|||", ",")
            user_agent = parse(transformed_text)
            device_type = 'mobile' if user_agent.is_mobile else 'tablet' if user_agent.is_tablet else 'pc' if user_agent.is_pc else 'other'
            is_touch_capable = 1 if user_agent.is_touch_capable == True else 0 if user_agent.is_touch_capable == False else 2 
                
            return ( user_agent.browser.family, user_agent.browser.version_string , user_agent.os.family , 
                        user_agent.os.version_string,  user_agent.device.family ,  user_agent.device.brand ,
                        user_agent.device.model , device_type , is_touch_capable )


        user_agent_infos_udf = udf( user_agent_infos , self.get_schema() )
        self.data_opens = self.data_opens.withColumn("value", split("value", ",")).selectExpr(
            "value[2] as job_id",
            "value[3] as rcpt_id",
            "value[4] as vmta_id",
            "cast(value[5] as Date) as date",
            "value[6] as request_ip",
            "value[8] as http_user_agent",
            "value[11] as list_id",
            "value[13] as isp_id"
        ).withColumn("user_agent", user_agent_infos_udf(col("http_user_agent")))
        self.data_opens = self.data_opens.select( 
            col("job_id") , col("date") , col("vmta_id") , col("list_id") , col("isp_id") ,  col("user_agent.*")
        ).filter("rcpt_id!='0'")


    @step(next=["join_actions_with_esp_domains_stream"])
    def esp_domains_read_as_stream(self):
        """
        Here we static dataframe from mysql
        after loading esp list we pass to next step
        - join_actions_with_esp_domains_stream
        """
        self.isp_domain_df = self.spark.read \
            .format("jdbc") \
            .option("driver","com.mysql.cj.jdbc.Driver") \
            .option("url", f'jdbc:mysql://{os.environ.get("DATABASE_HOST") or "DATABASE_HOST"}:{os.environ.get("DATABASE_PORT") or "DATABASE_PORT"}/joins') \
            .option("dbtable", "isp_domains") \
            .option("user", os.environ.get("DATABASE_USER") or "DATABASE_USER") \
            .option("password", os.environ.get("DATABASE_PASS") or "DATABASE_PASS") \
            .load()

    @step(next=["aggregate_stream"])
    def join_actions_with_esp_domains_stream(self):
        """
        Here we left join dynamic stream with static stream 
        after joining data we pass to next step
        - aggregate_stream
        """
        self.joined_combined_dstream = self.data_opens\
            .join(self.isp_domain_df, self.data_opens["isp_id"] == self.isp_domain_df["id"] , "left")\
            .select( 
                col("job_id") , col("date") , col("domain_name") , col("browser_family") , col("browser_version") , 
                col("os_family") , col("os_version") , col("device_family") , col("device_brand") , col("device_model") ,
                col("device_type") , col("device_touch_capable")
            )   

    @step(next=["write_stream"])
    def aggregate_stream(self):
        """
        Here we store result in our database mysql 
        after storing data we pass to next step
        - write_stream
        """
        self.joined_stream_aggregation = self.joined_combined_dstream \
            .groupBy(*self.joined_combined_dstream.columns) \
            .agg( count("*").alias("total_count") )
        

    @step(next=["wait_for_listners"])
    def write_stream(self):
        """
        Here we wait for Termination of listenrs. 
        and stop application if still running
        """
        # self.query = self.joined_stream_aggregation \
        #     .writeStream \
        #     .foreachBatch(self.write_to_mysql) \
        #     .outputMode("update")\
        #     .start()

        self.query =  self.joined_stream_aggregation.writeStream \
            .format("console") \
            .option("truncate" , False )\
            .outputMode("update")\
            .option("numRows",10000)\
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
    def write_to_mysql( self , df, _ ):
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
            sql = f"INSERT INTO drive_stats ( { colums } ) VALUES ( {values_string} ) ON DUPLICATE KEY UPDATE total_count = %s"
            # add on duplicate value
            # print(sql)
            values.append(values[ len(values) - 1 ])
            #print( values )
            try :
                cursor.execute(sql, values)
            except pymysql.err.DataError as e :  
                print(f" {str(e)} on {sql} , {values_string} ")
            except Exception as ex:
                print(f" error on {sql} , {values_string} , {str(ex)} ")
            except:
                print(f" error on {sql} , {values_string} ")
        cursor.close()
        connection.close()


    def user_agent_infos(self,text):
    # Perform your transformation logic here
        transformed_text = text.replace("|||", ",")
        user_agent = parse(transformed_text)
        device_type = 'mobile' if user_agent.is_mobile else 'tablet' if user_agent.is_tablet else 'pc' if user_agent.is_pc else 'other'
        is_touch_capable = 1 if user_agent.is_touch_capable == True else 0 if user_agent.is_touch_capable == False else 2 
            
        return ( user_agent.browser.family, user_agent.browser.version_string , user_agent.os.family , 
                    user_agent.os.version_string,  user_agent.device.family ,  user_agent.device.brand ,
                    user_agent.device.model , device_type , is_touch_capable )
    
    def get_schema(self):
        return StructType(
            [
                StructField('browser_family', StringType(), True),
                StructField('browser_version', StringType(), True),
                StructField('os_family', StringType(), True),
                StructField('os_version', StringType(), True),
                StructField('device_family', StringType(), True),
                StructField('device_brand', StringType(), True),
                StructField('device_model', StringType(), True),
                StructField('device_type', StringType(), True),
                StructField('device_touch_capable', IntegerType(), True)
            ]
        )