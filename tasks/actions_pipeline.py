try:

    import os , sys , pymysql
    from flowrunner import BaseFlow, end, start, step

    spark_version = '3.3.2'
    SUBMIT_ARGS = f'--packages ' \
                  f'org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version},' \
                  f'org.apache.kafka:kafka-clients:2.8.1,' \
                  f'mysql:mysql-connector-java:8.0.32 '\
                  f'pyspark-shell'

    os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
        
    import findspark
    findspark.init( '/opt/spark' )
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, udf
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    
    import geoip2.database
    
    print("ok.....")
except Exception as e:
    print("Error : {} ".format(e))



class ActionsPipelinePySpark(BaseFlow):
    spark = None
    fail_on_data_loss = "true"

    @start
    @step(next=[ "open_read_stream" , "click_read_stream" , "unsub_read_stream"  ])
    def start_spark_session(self):
        """
        This step is init spark session with and start application.
        connect with master node with name 'actions_pipeline' , reserve necessary resources and
        enable using sql in streaming 
        after Connecting to Master we pass to next steps 
        - open_read_stream
        - click_read_stream
        - unsub_read_stream
        """
        self.spark = SparkSession \
                .builder \
                .master(f"spark://{os.environ.get('MASTER_SPARK') or 'MASTER_SPARK'}:7077") \
                .appName("actions_pipeline") \
                .config("spark.executor.memory", "10g") \
                .config('spark.executor.cores', '10') \
                .config('spark.cores.max', '10') \
                .config("spark.sql.adaptive.enabled", "false") \
                .getOrCreate()
        

    @step(next=["transform_open_stream"])
    def open_read_stream(self):
        """
        Here we listen to kafka streaming topic 'open_actions'
        after listening to topic we pass to next step
        - transform_open_stream
        """
        self.data_open = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers",  f'{os.environ.get("KAFKA_HOST") or "KAFKA_HOST"}:{os.environ.get("KAFKA_PORT") or "KAFKA_PORT"}') \
            .option("subscribe", "open_actions") \
            .option("startingOffsets", "earliest") \
            .load()
        

    @step(next=["transform_click_stream"])
    def click_read_stream(self):
        """
        Here we listen to kafka streaming topic 'click_actions'
        after listening to topic we pass to next step
        - transform_click_stream
        """
        self.data_click = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers",  f'{os.environ.get("KAFKA_HOST") or "KAFKA_HOST"}:{os.environ.get("KAFKA_PORT") or "KAFKA_PORT"}') \
            .option("subscribe", "click_actions") \
            .option("startingOffsets", "earliest") \
            .load()
        
    @step(next=["transform_unsub_stream"])
    def unsub_read_stream(self):
        """
        Here we listen to kafka streaming topic 'unsub_actions'
        after listening to topic we pass to next step
        - transform_unsub_stream
        """
        self.data_unsub = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers",  f'{os.environ.get("KAFKA_HOST") or "KAFKA_HOST"}:{os.environ.get("KAFKA_PORT") or "KAFKA_PORT"}') \
            .option("subscribe", "unsub_actions") \
            .option("startingOffsets", "earliest") \
            .load()
        
    @step(next=["union_actions_stream"])
    def transform_open_stream(self):
        """     
        Here we transform data and extract necessary data 
        after trasform data we pass to next step
        - union_actions_stream
        """
        def get_ip_info(ip):
            with geoip2.database.Reader('/usr/share/GeoIP/GeoLite2-City.mmdb') as reader:
                response = reader.city(ip)
                country = response.country.iso_code 
            return country

        ip_info_udf = udf(get_ip_info, StringType())
        self.data_open = self.data_open.withColumn("value", split("value", ",")).selectExpr( 
            "value[2] as job_id",
            "value[3] as rcpt_id",
            "value[4] as vmta_id",
            "cast(value[5] as Date) as date",
            "value[6] as request_ip",
            "value[8] as http_user_agent",
            "value[11] as list_id",
            "value[13] as isp_id"
        ).withColumn("country", ip_info_udf(col("request_ip"))).select( 
            col("job_id") , col("date") , col("vmta_id") , col("country") , col("list_id") , col("isp_id") , lit('open').alias("type") 
        ).filter("rcpt_id!='0'")

    @step(next=["union_actions_stream"])
    def transform_click_stream(self):
        """     
        Here we transform data and extract necessary data 
        after trasform data we pass to next step
        - union_actions_stream
        """
        def get_ip_info(ip):
            with geoip2.database.Reader('/usr/share/GeoIP/GeoLite2-City.mmdb') as reader:
                response = reader.city(ip)
                country = response.country.iso_code 
            return country

        ip_info_udf = udf(get_ip_info, StringType())
        self.data_click = self.data_click.withColumn("value", split("value", ",")).selectExpr( 
            "value[2] as job_id",
            "value[3] as rcpt_id",
            "value[4] as vmta_id",
            "cast(value[5] as Date) as date",
            "value[6] as request_ip",
            "value[8] as http_user_agent",
            "value[11] as list_id",
            "value[13] as isp_id"
        ).withColumn("country", ip_info_udf(col("request_ip"))).select( 
            col("job_id") , col("date") , col("vmta_id") , col("country") , col("list_id") , col("isp_id") , lit('open').alias("type") 
        ).filter("rcpt_id!='0'")

    @step(next=["union_actions_stream"])
    def transform_unsub_stream(self):
        """     
        Here we transform data and extract necessary data 
        after trasform data we pass to next step
        - union_actions_stream
        """
        def get_ip_info(ip):
            with geoip2.database.Reader('/usr/share/GeoIP/GeoLite2-City.mmdb') as reader:
                response = reader.city(ip)
                country = response.country.iso_code 
            return country

        ip_info_udf = udf(get_ip_info, StringType())
        self.data_unsub = self.data_unsub.withColumn("value", split("value", ",")).selectExpr( 
            "value[2] as job_id",
            "value[3] as rcpt_id",
            "value[4] as vmta_id",
            "cast(value[5] as Date) as date",
            "value[6] as request_ip",
            "value[8] as http_user_agent",
            "value[11] as list_id",
            "value[13] as isp_id"
        ).withColumn("country", ip_info_udf(col("request_ip"))).select( 
            col("job_id") , col("date") , col("vmta_id") , col("country") , col("list_id") , col("isp_id") , lit('open').alias("type") 
        ).filter("rcpt_id!='0'")


    @step(next=["esp_domains_read_as_stream","read_list_names_as_stream"])
    def union_actions_stream(self):
        """
        Here we merge the three streames together
        after merging data we pass to next step
        - esp_domains_read_as_stream
        - read_list_names_as_stream
        """
        self.combined_dstream = self.data_open.union(self.data_click).union(self.data_unsub)


    @step(next=["join_actions_with_esp_and_list_stream"])
    def esp_domains_read_as_stream(self):
        """
        Here we read static dataframe from database as stream
        after reading from database we pass to next step
        - join_actions_with_esp_and_list_stream
        """
        self.isp_domain_df = self.spark.read \
            .format("jdbc") \
            .option("driver","com.mysql.cj.jdbc.Driver") \
            .option("url", f'jdbc:mysql://{os.environ.get("DATABASE_HOST") or "DATABASE_HOST"}:{os.environ.get("DATABASE_PORT") or "DATABASE_PORT"}/joins') \
            .option("dbtable", "isp_domains") \
            .option("user", os.environ.get("DATABASE_USER") or "DATABASE_USER") \
            .option("password", os.environ.get("DATABASE_PASS") or "DATABASE_PASS") \
            .load()
        
    @step(next=["join_actions_with_esp_and_list_stream"])
    def read_list_names_as_stream(self):
        """
        Here we read static dataframe from database as stream
        after reading from database we pass to next step
        - join_actions_with_esp_and_list_stream
        """
        self.lists_df = self.spark.read \
            .format("jdbc") \
            .option("driver","com.mysql.cj.jdbc.Driver") \
            .option("url", f'jdbc:mysql://{os.environ.get("DATABASE_HOST") or "DATABASE_HOST"}:{os.environ.get("DATABASE_PORT") or "DATABASE_PORT"}/joins') \
            .option("dbtable", "lists") \
            .option("user", os.environ.get("DATABASE_USER") or "DATABASE_USER") \
            .option("password", os.environ.get("DATABASE_PASS") or "DATABASE_PASS") \
            .load()

    @step(next=["aggregate_joined_stream"])
    def join_actions_with_esp_and_list_stream(self):
        """
        Here we left join dynamic stream with two static streams 
        after joining data we pass to next step
        - aggregate_joined_stream
        """
        self.joined_combined_dstream = self.combined_dstream\
            .join(self.isp_domain_df, self.combined_dstream["isp_id"] == self.isp_domain_df["id"] , "left")\
            .join(self.lists_df, self.combined_dstream["list_id"] == self.lists_df["id"] , "left") \
            .select( 
                col("job_id") , col("date") , col("vmta_id") ,  col("country") , 
                col("domain_name") , col("code").alias("list_name") , col("type")
            )
    
    @step(next=["write_stream"])
    def aggregate_joined_stream(self):
        """
        Here we aggregate data count group by operation
        after aggregating data we pass to next step
        - write_stream
        """
        self.joined_combined_aggregation_dstream = self.joined_combined_dstream\
        .groupBy(*self.joined_combined_dstream.columns) \
        .agg( count("*").alias("total_count") )


    @step(next=["wait_for_listners"])
    def write_stream(self):
        """
        Here we store the result of pipeline. 
        after storing data we pass to next step
        - wait_for_listners
        """
        #Write the streaming DataFrame to PostgreSQL using foreachBatch
        self.query = self.joined_combined_aggregation_dstream \
            .writeStream \
            .foreachBatch(self.write_to_mysql) \
            .outputMode("update")\
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
            sql = f"INSERT INTO actions_table ( { colums } ) VALUES ( {values_string} ) ON DUPLICATE KEY UPDATE total_count = %s"
            # add on duplicate value
            # print(sql)
            values.append(values[ len(values) - 1 ])
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