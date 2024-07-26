import logging
import os
import datetime
from csv_logger import CsvLogger

# Définir project_name une fois
project_name = os.path.basename(os.getcwd())

# Configuration du logger
logger = CsvLogger(
    filename='/home/support-info/TM/09-monitoring-global/logs.csv',
    delimiter=',',
    level=logging.INFO,
    fmt='%(asctime)s,%(levelname)s,%(message)s,%(project_name)s,%(status)s,%(duration)s',
    datefmt='%Y/%m/%d %H:%M:%S',
    header=['date', 'levelname', 'message', 'project_name', 'status', 'duration']
)

# Capture initial timestamp
start_time = datetime.datetime.now()

# Fonction pour obtenir le statut en fonction du niveau de log
def get_status(level):
    if level == 'INFO':
        return 'success'
    elif level == 'ERROR':
        return 'failed'
    elif level == 'WARNING':
        return 'warning'
    return 'unknown'

# Fonction pour ajouter des logs en fonction du message reçu
def log_message(logger, message, level):
    current_time = datetime.datetime.now()
    duration_seconds = (current_time - start_time).total_seconds()

    status = get_status(level)

    log_entry = {
        'project_name': project_name,
        'status': status,
        'duration': f"{duration_seconds:.4f}s"  # Formater la durée avec 4 décimales

    }

    logger.log(level=getattr(logging, level.upper()), msg=message, extra=log_entry)

# Simuler l'ajout des logs
logs = [
    "2024-07-25 11:49:26 - INFO - Start timestamp: 2024-07-25 11:49:26.376470",
    "2024-07-25 11:49:33 - INFO - Conversion has been started!",
    "2024-07-25 11:49:33 - INFO - Conversion process completed.",
    "2024-07-25 11:49:37 - ERROR - No valid parquet files found in the directory.",
    "2024-07-25 11:49:55 - INFO - Traitement finished timestamp: 2024-07-25 11:49:52.188788",
    "2024-07-25 11:50:02 - INFO - Duration job: 0h:0m:29s"
]

log_directory = '/home/support-info/TM/'
all_items = os.listdir(log_directory)
directories = [item for item in all_items if os.path.isdir(os.path.join(log_directory, item))]

log_file_paths = []

# Explorer chaque dossier pour trouver les fichiers de log
for directory in directories:
    logs_directory = os.path.join(log_directory, directory, 'logs')
    
    # Vérifier si le sous-dossier 'logs' existe
    if os.path.isdir(logs_directory):
        log_files = [file for file in os.listdir(logs_directory) if file.endswith('.log')]
        
        # Ajouter les chemins complets des fichiers de log à la liste
        for log_file in log_files:
            full_log_file_path = os.path.join(logs_directory, log_file)
            log_file_paths.append(full_log_file_path)
            
for log in log_file_paths:
    parts = log.split(' - ')
    log_message(logger, parts[2], parts[1])

print("Logs have been written to logs.csv")