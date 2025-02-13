import sys

import logging
import logging.handlers


class PipelineLogs:

    def __init__(self, name, file_name, console:bool = True, file:bool = True):
        
        self.log_file = file_name
        self.loggers = logging.getLogger(name)
        self.loggers.setLevel(logging.DEBUG)

        if len(self.loggers.handlers)==0:
            self.__log_definition(console, file)

    def __log_definition(self, console:bool = True, file:bool = True):

        formatter = logging.Formatter(
            "[LINE:%(lineno)d] #%(levelname)-8s [%(asctime)s] %(message)s"
        )

        if file:

            file_handler = logging.handlers.RotatingFileHandler(
                filename=self.log_file, maxBytes=1024000, backupCount=5
            )
            file_handler.setFormatter(formatter)
            self.loggers.addHandler(file_handler)

        if console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            self.loggers.addHandler(console_handler)

    def pipeline_logs(self):

        return self.loggers