from dotenv import load_dotenv
load_dotenv() 

from tasks.logs_pipeline import LogsPipelinePySpark

try :
    LogsPipelinePySpark().run()
except Exception as e :
    print(f"There is an Error apears in {str(e)}")