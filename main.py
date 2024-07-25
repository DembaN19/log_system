from datetime import datetime
import logging as logger
from src.utils import *

def main():
    start_timestamp = datetime.now()
    logger.info(f'Start timestamp: {datetime.now()}')

    schema_table = config.db_dwh.schema_table
    # Convert to parquet 
    convert_csv_to_parquet(config.pathsfolders.srcfiles, config.pathsfolders.src_parquet)
    
    # Read CSV files 
    df = read_with_parquet(config.pathsfolders.src_parquet, config.pathsfolders.archfiles_parquet)
    
    if len(df) > 0:
        # Inserting data using our function
        insert_data_into_db(df, f'{schema_table}.{table_name}', server, database, username, password)
        
        # After insertion move file to arch
        move_files_csv_to_archive(config.pathsfolders.srcfiles, config.pathsfolders.archfiles_csv)
        move_files_parquet_to_archive(config.pathsfolders.src_parquet, config.pathsfolders.archfiles_parquet)
    

    

    # Time duration
    end_timestamp = datetime.now()
    logger.info(f'Traitement finished timestamp: {end_timestamp}')
    traitement_duration = diff_time(start_timestamp , end_timestamp)
    logger.info(f'Duration job: {traitement_duration[0]}h:{traitement_duration[1]}m:{traitement_duration[2]}s ')
    
    log_entries = [
        {
            "project_name": os.path.basename(os.getcwd()),
            "status": "completed",
            "duration": f'{traitement_duration[0]}h:{traitement_duration[1]}m:{traitement_duration[2]}s'
        }
    ]
    
    csv_file_path = '/home/support-info/TM/09-monitoring-global/log.csv'
    generate_log_and_write_csv(log_entries, csv_file_path)

if __name__ == "__main__":
    main()
    