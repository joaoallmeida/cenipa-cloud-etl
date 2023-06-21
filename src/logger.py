import logging
import logstash

class Logger(object):

    def __init__(self, logger_name:str, logstash_host:str='localhost', logstash_port:int = 5959) -> None:
        self.logger_name = logger_name
        self.logstash_host = logstash_host
        self.logstash_port = logstash_port
        
    def config(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s -> %(message)s')
        self.logger = logging.getLogger(self.logger_name)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(logstash.LogstashHandler(self.logstash_host, self.logstash_port, version=1))

        return self.logger