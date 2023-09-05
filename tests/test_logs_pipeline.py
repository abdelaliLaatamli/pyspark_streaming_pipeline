import unittest
from tasks.logs_pipeline import LogsPipelinePySpark

class TestLogsPipelinePySpark(unittest.TestCase):

    def assertNoRaise(self, func, *args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception as e:
            print( "---------------" , e)
            if e :
                self.fail(f"Exception raised: {e}")

    def test_step1(self):
        workflow = LogsPipelinePySpark()
        
        # Instantiate the workflow
        # workflow = LogsPipelinePySpark()

        # # Call the method for step 1
        # result = workflow.step1()

        # # Add assertions to check the result
        # self.assertEqual(result, expected_result)
        # self.assertEqual(False , True)
        #self.assertNotR(False , True)
        self.assertNoRaise( workflow.start_spark_session() )


    def test_step2(self):
        # Similar to test_step1, test step 2
        pass

    def test_step3(self):
        # Similar to test_step1, test step 3
        pass

