import configparser
import json
import boto3
import traceback
import os

from etl.logger import Logger, extra_fields, Status, Step
from etl.extract import Extract
from etl.refined import Refined
from etl.load import Load
from events.event import extract_event, refine_event, event, load_event


def lambda_handler(event,context):

    aws_session = boto3.Session(region_name='us-east-1')
    bucket = os.environ['s3_bucket']
    
    try:
        logInstance = Logger( project_name='cenipa-etl', aws_session=aws_session , drop=False)
        logger = logInstance.config()

        logger.info('Starting ETL', extra=extra_fields(Step.START, Status.INITING))

        extInstance = Extract(logger_session=logger, aws_session=aws_session, bucket=bucket)
        rfInstance = Refined(logger_session=logger, aws_session=aws_session, bucket=bucket)
        ldInstance = Load(logger_session=logger, aws_session=aws_session, bucket=bucket)

        for pipe in event['pipeline']:
            if pipe['step'] == 'extract':
                extInstance.get_raw_data( pipe['tables'] )
            elif pipe['step'] == 'refined':
                rfInstance.refine_data( pipe['tables'], pipe['date_ref'] )
            elif pipe['step'] == 'load':
                ldInstance.load_starschema_model(tables=pipe['tables'], dt_ref=pipe['date_ref'], create_ddl=pipe['create_ddl'])
            else:
                logger.warning('Step not mapped')

        logger.info('Finished ETL ', extra=extra_fields(Step.END, Status.COMPLETED))

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

    # print(lambda_handler(extract_event,None))
    # print(lambda_handler(refine_event,None))
    # print(lambda_handler(load_event,None))
    # print(lambda_handler(event,None))