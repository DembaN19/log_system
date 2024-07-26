import logging
import os
import datetime
from csv_logger import CsvLogger

# Configuration du logger avec un chemin fixe pour le fichier CSV
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
def log_message(logger, message, level, start_time, project_name):
    current_time = datetime.datetime.now()
    duration_seconds = (current_time - start_time).total_seconds()

    status = get_status(level)

    log_entry = {
        'project_name': project_name,
        'status': status,
        'duration': f"{duration_seconds:.4f}s"  # Formater la durée avec 4 décimales
    }

    # Log message avec des informations supplémentaires
    logger.log(level=getattr(logging, level.upper()), msg=message, extra=log_entry)

# Fonction pour extraire le project_name à partir du chemin du fichier de log
def extract_project_name(log_file_path):
    # Extrait le nom du répertoire parent juste après 'TM/'
    path_parts = log_file_path.split('/')
    project_index = path_parts.index('TM') + 1
    return path_parts[project_index]

def main():
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

    # Traiter chaque fichier de log
    for log_file_path in log_file_paths:
        project_name = extract_project_name(log_file_path)
        
        with open(log_file_path, 'r') as log_file:
            for line in log_file:
                # Découper la ligne du log
                parts = line.strip().split(' - ')
                if len(parts) >= 3:
                    timestamp, level, message = parts[0], parts[1], ' - '.join(parts[2:])
                    log_message(logger, message, level, start_time, project_name)

    print("Logs have been written to logs.csv")

if __name__ == "__main__":
    main()