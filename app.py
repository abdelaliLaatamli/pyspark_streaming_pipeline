from dotenv import load_dotenv
load_dotenv() 

from tasks import LogsPipelinePySpark , UserAgentPipelinePySpark , ActionsPipelinePySpark

try :
    # LogsPipelinePySpark().run()
    ActionsPipelinePySpark().run()
    # UserAgentPipelinePySpark().run()
except Exception as e :
    print(f"There is an Error apears in {str(e)}")