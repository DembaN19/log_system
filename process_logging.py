import os
import logging
from time import sleep
from csv_logger import CsvLogger
import pandas as pd

# Get the current directory name as the project name
project_name = os.path.basename(os.getcwd())

filename = 'log.csv'
delimiter = ';'
level = logging.INFO
custom_additional_levels = ['logs_a', 'logs_b', 'logs_c']
custom_additional_level_nums = [logging.INFO + 1, logging.INFO + 2, logging.INFO + 3]
fmt = f'%(asctime)s{delimiter}%(levelname)s{delimiter}%(message)s'
datefmt = '%Y/%m/%d %H:%M:%S'
max_size = 1024  # 1 kilobyte
max_files = 4  # 4 rotating files
header = ['date', 'level', 'value_1', 'value_2', 'project_name', 'status']

class CustomCsvLogger(CsvLogger):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.custom_level_mapping = dict(zip(custom_additional_levels, custom_additional_level_nums))
        logging.addLevelName(self.custom_level_mapping['logs_a'], 'LOGS_A')
        logging.addLevelName(self.custom_level_mapping['logs_b'], 'LOGS_B')
        logging.addLevelName(self.custom_level_mapping['logs_c'], 'LOGS_C')

    def logs_a(self, message):
        self._log_custom('logs_a', message)

    def logs_b(self, message):
        self._log_custom('logs_b', message)

    def logs_c(self, message):
        self._log_custom('logs_c', message)

    def _log_custom(self, level_name, message):
        # Get the level number for the custom level name
        level_num = self.custom_level_mapping[level_name]
        # Determine the status based on log level
        status = 'failed' if level_num >= logging.ERROR else 'success'

        # Format the message
        if isinstance(message, list):
            message = delimiter.join(map(str, message))

        # Log the message with project name and status as extra
        extra = {'project_name': project_name, 'status': status}
        self._log(level_num, message, extra=extra)

# Create logger with custom csv rotating handler
csvlogger = CustomCsvLogger(filename=filename,
                            delimiter=delimiter,
                            level=level,
                            add_level_names=custom_additional_levels,
                            add_level_nums=custom_additional_level_nums,
                            fmt=fmt,
                            datefmt=datefmt,
                            max_size=max_size,
                            max_files=max_files,
                            header=header)

# Log some records
for i in range(10):
    csvlogger.logs_a([i, i * 2])
    sleep(0.1)

# You can log list or string
csvlogger.logs_b([1000.1, 2000.2])
csvlogger.critical('3000,4000')

# Log some more records to trigger rollover
for i in range(50):
    csvlogger.logs_c([i * 2, float(i**2)])
    sleep(0.1)

# Read and print all of the logs from file after logging
all_logs = csvlogger.get_logs(evaluate=False)
for log in all_logs:
    print(log)