import logging
import logstash
import enum


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

class Logger(object):

    def __init__(self, project_name:str, logstash_host:str='localhost', logstash_port:int = 5959) -> None:
        self.project_name = project_name
        self.logstash_host = logstash_host
        self.logstash_port = logstash_port
        self.logger = None
        
    def start(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s -> %(message)s')
        self.logger = logging.getLogger(self.project_name)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(logstash.LogstashHandler(self.logstash_host, self.logstash_port, message_type='python-etl',version=1))

        self.logger.info('Init', extra=self.extra_fields(Step.START, Status.PROCESSING))

        return self.logger
    
    def end(self, status:enum.Enum):

        if self.logger is None:
            raise Exception("Logger has not been initialized.")
        
        self.logger.info('End', extra=self.extra_fields(Step.END, status))


    def extra_fields(self, step:enum.Enum, status:enum.Enum, table:str='N/D', lines:int=0 ) -> dict:
         
        data = {
            "table_name": table,
            "status": status.value,
            "execution_step": step.value,
            "num_affected_lines":lines,
        }

        return data