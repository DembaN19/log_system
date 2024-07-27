import logging
import os
import datetime
import re
from csv_logger import CsvLogger
import pandas as pd
from src.utils import config, insert_data_into_db, drop_data, server, database, table_name, username, password
from decorators.timing import get_time

# Configuration du logger avec un chemin fixe pour le fichier CSV
logger = CsvLogger(
    filename='/home/support-info/TM/09-monitoring-global/logs.csv',
    delimiter=',',
    level=logging.INFO,
    fmt='%(date)s,%(levelname)s,%(message)s,%(project_name)s,%(status)s,%(duration)s,%(load_file)s',
    datefmt='%Y/%m/%d %H:%M:%S',
    header=['date', 'levelname', 'message', 'project_name', 'status', 'duration', 'load_file']
)
schema_table = config.db_dwh.schema_table


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

# Clean file
df = pd.read_csv("logs.csv")
df_vide = pd.DataFrame(columns=df.columns)
df_vide.to_csv("/home/support-info/TM/09-monitoring-global/logs.csv", index=False)
# Fonction pour ajouter des logs en fonction du message reçu
def log_message(logger, timestamp, message, level, start_time, project_name):
    current_time = datetime.datetime.now()
    duration_seconds = (current_time - start_time).total_seconds()
    date_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    status = get_status(level)

    log_entry = {
        'date': timestamp,
        'project_name': project_name,
        'status': status,
        'duration': f"{duration_seconds:.4f}",  # Formater la durée avec 4 décimales
        'load_file': date_time
    }
    message = message.replace(',', '')
    if len(message) > 42:
        message = message[:42]
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

    # Définir un motif regex pour extraire le timestamp, le niveau et le message
    log_pattern = re.compile(r"(\d{4}[-/]\d{2}[-/]\d{2} \d{2}:\d{2}:\d{2})\s*[-,]?\s*(INFO|ERROR|WARNING)\s*[-,]?\s*(.*)", re.DOTALL)


    # Traiter chaque fichier de log
    for log_file_path in log_file_paths:
        project_name = extract_project_name(log_file_path)
        
        with open(log_file_path, 'r') as log_file:
            for line in log_file:
                try:
                    # Appliquer le motif regex à la ligne
                    match = re.search(log_pattern, line.strip())
                    if match:
                        timestamp, level, message = match.groups()
                        if level in ['INFO', 'ERROR', 'WARNING']:
                            log_message(logger, timestamp, message, level, start_time, project_name)
                    
                except Exception as e:
                    print(f"Error processing line in {log_file_path}: {line}\n{e}")

    print("Logs have been written to logs.csv")
    drop_data(server, database, username, password, f'{schema_table}.{table_name}')
    df = pd.read_csv("logs.csv")
    df['duration'] = df['duration'].astype(float)
    df['date'] = pd.to_datetime(df['date'])
    df['load_file'] = pd.to_datetime(df['load_file'])

    # Extraire la date seulement
    df['date_only'] = df['date'].dt.date

    # Trier par 'project_name', 'date_only', et 'date'
    df = df.sort_values(by=['project_name', 'date_only', 'date']).reset_index(drop=True)

    # Calculer la différence en secondes entre les lignes dans chaque groupe
    df['duration'] = df.groupby(['project_name', 'date_only'])['date'].diff().dt.total_seconds()

    # Formater la colonne 'duration' pour avoir 4 chiffres après la virgule
    df['duration'] = df['duration'].apply(lambda x: f"{x:.4f}" if pd.notnull(x) else 0)
    df = df.drop(columns='date_only')
    insert_data_into_db(df, f'{schema_table}.{table_name}', server, database, username, password)
    print('job done')
    

if __name__ == "__main__":
    main()