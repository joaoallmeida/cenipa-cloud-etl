from logging import NOTSET, Handler, LogRecord
from datetime import datetime
import logging
import enum
import pymongo


class Status(enum.Enum):
    PROCESSING = 'P'
    COMPLETED = 'C'
    FAILURE = 'F'
    ERROR = 'E'
    SUCCESSFULLY = 'S'


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
    UPLOAD = 'UPLOAD'


def extra_fields(step:enum.Enum, status:enum.Enum, table:str='N/D', lines:int=0 ) -> dict:
    data = {
        "table_name": table,
        "status": status.value,
        "execution_step": step.value,
        "num_affected_lines":lines,
    }
    return data

class MongoHandler(Handler):

    def __init__(self, host:str, port:int=27017, database:str='logdb', collection:str='log', drop:bool=False) -> None:

        Handler.__init__(self)
        self.conn = pymongo.MongoClient(host=host, port=port)
        self.database = self.conn[database]

        if collection in self.database.list_collection_names():
            if drop:
                self.database.drop_collection(collection)
                self.collection = self.database.create_collection(collection)
            else:
                self.collection = self.database[collection]
        else:
            self.collection = self.database.create_collection(collection)
    
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
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
                "level": record.levelno,
                "level_name": record.levelname,
                "message": record.getMessage(),
                "process_name": record.name,
                "file_name": record.filename,
                "path_name": record.pathname
            }
        
        # Add extra fields
        message.update(self.get_extra_fields(record))

        self.collection.insert_one( message )


class Logger(object):

    def __init__(self, project_name:str, host:str, port:int=27017, database:str='logdb', collection:str='log', drop:bool=False) -> None:
        self.project_name = project_name
        self.host = host
        self.port = port
        self.database = database
        self.drop = drop
        self.logger = None
        
        self.collection = project_name if collection == 'log' else collection
        
    def start(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s -> %(message)s')
        self.logger = logging.getLogger(self.project_name)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(MongoHandler( self.host, self.port, self.database, self.collection, self.drop ))

        # self.logger.info('Init', extra=extra_fields(Step.START, Status.PROCESSING))

        return self.logger
    
    def end(self, status:enum.Enum):

        if self.logger is None:
            raise Exception("Logger has not been initialized.")
        
        self.logger.info('End', extra=extra_fields(Step.END, status))