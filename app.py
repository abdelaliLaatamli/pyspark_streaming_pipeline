from dotenv import load_dotenv
load_dotenv() 

from tasks import LogsPipelinePySpark , UserAgentPySpark

try :
    # LogsPipelinePySpark().run()
    UserAgentPySpark().run()
except Exception as e :
    print(f"There is an Error apears in {str(e)}")