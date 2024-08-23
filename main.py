import logging
import os
from datetime import datetime
from csv_logger import CsvLogger
from src.utils import *



def main():
    # Capture initial timestamp
    start_time = datetime.now()
    current_dir = os.getcwd()
    schema_table = config.db_dwh.schema_table
    log_directory = config.pathsfolders.log_directory
    
    # Configuration du logger avec un chemin fixe pour le fichier CSV
    logger_csv = CsvLogger(
        filename=f'{current_dir}/logs.csv',
        delimiter=',',
        level=logging.INFO,
        fmt='%(date)s,%(levelname)s,%(message)s,%(project_name)s,%(status)s,%(duration)s,%(load_file)s',
        datefmt='%Y/%m/%d %H:%M:%S',
        header=['date', 'levelname', 'message', 'project_name', 'status', 'duration', 'load_file']
    )
    # Clean file
    reset_csv_file(f'{current_dir}/logs.csv')
    process_log_files(log_directory, logger_csv, start_time)
    
    drop_data(server, database, username, password, f'{schema_table}.{table_name}')
    df = process_logs(f'{current_dir}/logs.csv')
    insert_data_into_db(df, f'{schema_table}.{table_name}', server, database, username, password)
    
    
    
    
    end_timestamp = datetime.now()
    logger.info(f'Traitement finished timestamp: {end_timestamp}')
    traitement_duration = diff_time(start_time , end_timestamp)
    logger.info(f'Duration job: {traitement_duration[0]}h:{traitement_duration[1]}m:{traitement_duration[2]}s ')
    

if __name__ == "__main__":
    main()
