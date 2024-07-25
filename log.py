import logging
import pandas as pd
from datetime import datetime
import time
import os

def generate_log_and_write_csv(log_entries, csv_file_path):
    # Configuration du logger
    logger = logging.getLogger('my_logger')
    logger.setLevel(logging.DEBUG)
    
    # Formatter pour le logger
    formatter = logging.Formatter('%(asctime)s, %(levelname)s,%(message)s')
    
    # Handler pour écrire les logs dans un fichier temporaire
    log_file_path = 'logs/temporary_logfile.log'
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Fonction pour logguer les informations
    def log_project_info(project_name, status, duration):
        
        message = f'{project_name},{status},{duration}'
        logger.info(message)
    
    # Enregistrer les logs des projets
    for entry in log_entries:
        log_project_info(entry['project_name'], entry['status'], entry['duration'])
        time.sleep(1)  # Simule une attente entre les logs
    
    # Lecture des logs et écriture dans un fichier CSV
    # Lire le fichier de log
    log_df = pd.read_csv(log_file_path, names=['date', 'levelname', 'project_name', 'status', 'duration'], sep=',')
    
    log_df['date'] = datetime.now().strftime("%Y-%m-%d")
    # Écrire dans un fichier CSV
    log_df.to_csv(csv_file_path, mode='a', header=False, index=False)
    

    

log_entries = [
        {
            "project_name": os.path.basename(os.getcwd()),
            "status": "completed",
            "duration": '2024-07-24 14:41:25'
        }
    ]
    
# Appeler la fonction pour générer et écrire les logs
csv_file_path = '/home/support-info/TM/09-monitoring-global/log.csv'
generate_log_and_write_csv(log_entries, csv_file_path)