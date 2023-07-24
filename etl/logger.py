import hashlib
from logging import NOTSET, Handler, LogRecord
from datetime import datetime
import logging
import enum
import socket
import getpass
import boto3
import time

class Status(enum.Enum):
    PROCESSING = 'P'
    COMPLETED = 'C'
    FAILURE = 'F'
    ERROR = 'E'
    SUCCESSFULLY = 'S'
    INITING = 'I'


class Step(enum.Enum):
    START = 'START'
    END = 'END'
    EXTRACT_INIT = 'EXTRACT_INIT'
    EXTRACT_END = 'EXTRACT_END'
    REFINED_INIT = 'REFINED_INIT'
    REFINED_END= 'REFINED_END'
    LOAD_INIT = 'LOAD_INIT'
    LOAD_END = 'LOAD_END'
    READ = 'READ'
    REFINE = 'REFINE'
    CREATE = 'CREATE'
    INSERT = 'INSERT'
    UPDATE = 'UPDATE'
    UPLOAD = 'UPLOAD'


def extra_fields(step:enum.Enum, status:enum.Enum, table:str='N/D', lines:int=0, storage:str='dynamoDb') -> dict:
    if storage != 'dynamoDb':
        data = {
            "table_name": table,
            "status": status.value,
            "execution_step": step.value,
            "num_affected_lines":lines,
        }
    else:
        data = {
            "table_name": {"S":table},
            "status": {"S":status.value},
            "execution_step": {"S":step.value},
            "num_affected_lines":{"N":str(lines)},
        }
    return data


class DynamoDbHandler(Handler):

    def __init__(self, aws_session:boto3.Session, table_name:str, drop:bool=False) -> None:

        Handler.__init__(self)
        self.dynamo_client = aws_session.client('dynamodb')

        if table_name in self.dynamo_client.list_tables()['TableNames']:
            self.table_name = table_name
        else:
            raise ValueError(f'Table {table_name} not found!')

    def get_extra_fields(self, record):
        # The list contains all the attributes listed in
        # http://docs.python.org/library/logging.html#logrecord-attributes

        skip_list = (
            'args', 'asctime', 'created', 'exc_info', 'exc_text', 'filename',
            'funcName', 'id', 'levelname', 'levelno', 'lineno', 'module',
            'msecs', 'msecs', 'message', 'msg', 'name', 'pathname', 'process',
            'processName', 'relativeCreated', 'thread', 'threadName', 'extra',
            'auth_token', 'password', 'stack_info')
        
        easy_types = (str, bool, dict, float, int, list, type(None))
        fields = {}

        for key, value in record.__dict__.items():
            if key not in skip_list:
                if isinstance(value, easy_types):
                    fields[key] = value
                else:
                    fields[key] = repr(value)

        return fields
    
    def emit(self, record:LogRecord) -> None:
        message =  {
                "object_id": {"S": hashlib.sha256(str(datetime.now().strftime('%H%M%S%f')).encode('ascii') ).hexdigest() },
                "timestamp": { "S": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') },
                "hostname": {"S": socket.gethostname()},
                "host": {"S": socket.gethostbyname(socket.gethostname())},
                "user": {"S": getpass.getuser()},
                "level": {"N": str(record.levelno)},
                "level_name": {"S": record.levelname},
                "message": {"S": record.getMessage()},
                "process_name": {"S": record.name},
                "file_name": {"S": record.filename},
                "path_name": {"S": record.pathname}
            }
        
        # Add extra fields
        message.update(self.get_extra_fields(record))

        try:
            self.dynamo_client.put_item(
                TableName=self.table_name,
                Item=message
            )
        except Exception as e:
            raise e

class Logger(object):

    def __init__(self, project_name:str, aws_session:boto3.Session, table_name:str=None, drop:bool=False) -> None:
        self.project_name = project_name
        self.aws_session = aws_session
        self.drop = drop
        self.table_name = project_name + "-log" if table_name is None else table_name + "-log"
        
    def config(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s -> %(message)s')
        self.logger = logging.getLogger(self.project_name)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(DynamoDbHandler( self.aws_session, self.table_name, self.drop ))
        return self.logger