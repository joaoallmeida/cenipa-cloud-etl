import configparser
import json
import polars as pl
import boto3
import traceback

from etl.logger import Logger, Status, Step
from etl.extract import Extract
from etl.refined import Refined
from tests.event import extract_event, refine_event


def lambda_handler(event,context):

    conf = configparser.ConfigParser()
    conf.read('config/credentials.ini')
    mongodb_host = conf['MONGODB']['host']

    aws_session = boto3.Session(region_name='us-east-1')
    bucket = conf['AWS']['bucket']
    
    try:
        logInstance = Logger(project_name='cenipa-etl', host=mongodb_host)
        logger = logInstance.start()

        extInstance = Extract(logger_session=logger, aws_session=aws_session, bucket=bucket)
        rfInstance = Refined(logger_session=logger, aws_session=aws_session, bucket=bucket)

        if event['step'] == 'extract':
            extInstance.get_raw_data( event['tables'] )
        elif event['step'] == 'refined':
            rfInstance.refine_data( event['tables'], event['date_ref'] )
        else:
            pass

        # logInstance.end(Status.SUCCESSFULLY)

    except Exception as e:
        logger.error(f'Pipeline failure: {traceback.format_exc()}')
        raise e
    else:
        return {
            "statusCode": 200,
            "message": json.dumps({"execution":"completed"})
        }

# To localhost test, uncomment this code snippet
# if __name__=="__main__":

#     # print(lambda_handler(extract_event,None))
#     print(lambda_handler(refine_event,None))