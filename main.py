import configparser
import json
import boto3
import traceback

from etl.logger import Logger, extra_fields, Status, Step
from etl.extract import Extract
from etl.refined import Refined
from etl.load import Load
from events.event import extract_event, refine_event, event, load_event


def lambda_handler(event,context):

    conf = configparser.ConfigParser()
    conf.read('config/credentials.ini')

    aws_session = boto3.Session(region_name='us-east-1')
    bucket = conf['AWS']['bucket']
    
    try:
        logInstance = Logger( project_name='cenipa-etl', aws_session=aws_session, drop=True )
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
                ldInstance.load_starschema_model(pipe['tables'], pipe['date_ref'])
            else:
                logger.warning('Step not mapped')

        logger.info('Finished ETL ', extra=extra_fields(Step.END, Status.COMPLETED))

    except Exception as e:
        logger.error(f'Pipeline failure: {traceback.format_exc()}')
        raise {
            "statusCode": 400,
            "message": traceback.format_exc()
        }
    
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