import unittest
from unittest.mock import patch, Mock
from tasks.logs_pipeline import LogsPipelinePySpark
from pyspark.sql import SparkSession

class TestLogsPipelinePySpark(unittest.TestCase):

    def assertNoRaise(self, func, *args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception as e:
            if e is None :
                self.fail(f"Exception raised: {e}")

    def test_step1(self):
        workflow = LogsPipelinePySpark()
        self.assertNoRaise( workflow.start_spark_session() )


    @patch("tasks.logs_pipeline.SparkSession")
    @patch("os.environ", {"KAFKA_HOST": "localhost", "KAFKA_PORT": "9092"})
    def test_bounce_read_stream(self, mock_spark_session):
        # Create a Mock SparkSession
        mock_spark = mock_spark_session.builder.getOrCreate.return_value

        # Create an instance of LogsPipelinePySpark
        logs_pipeline = LogsPipelinePySpark()
        # Mock the SparkSession's readStream method
        mock_read_stream = mock_spark.readStream.format.return_value.option.return_value.option.return_value.option.return_value.load.return_value
        mock_read_stream.createDataFrame.return_value = Mock()

        # Call the bounce_read_stream method
        logs_pipeline.bounce_read_stream()

        # Assert that the readStream method was called with the expected arguments
        mock_spark.readStream.format.assert_called_once_with("kafka")
        mock_read_stream.option.assert_any_call("kafka.bootstrap.servers", "localhost:9092")
        mock_read_stream.option.assert_any_call("subscribe", "pmta-bounce")
        mock_read_stream.option.assert_any_call("startingOffsets", "earliest")
        mock_read_stream.option.assert_any_call("failOnDataLoss", "true")

        # Assert that createDataFrame was called
        mock_read_stream.createDataFrame.assert_called_once()

    def test_step3(self):
        # Similar to test_step1, test step 3
        pass

