import logging
import pandas as pd
from datetime import datetime
import time
import glob
import os

def generate_log_and_write_csv(log_directory, csv_file_path, duration):
    # Configuration du logger
    logger = logging.getLogger('my_logger')
    logger.setLevel(logging.DEBUG)
    
    # Formatter pour le logger
    formatter = logging.Formatter('%(asctime)s, %(levelname)s,%(message)s')
    
    # Handler pour écrire les logs dans un fichier temporaire
    temp_log_file_path = 'temporary_logfile.log'
    file_handler = logging.FileHandler(temp_log_file_path)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Fonction pour logguer les informations
    def log_project_info(project_name, status, duration):
        message = f'{project_name},{status},{duration}'
        logger.info(message)
    
    # Exemple d'entrées de log pour le test
    log_entries = [
        {
            "project_name": os.path.basename(os.getcwd()),
            "status": "success",
            "duration": duration
        }
    ]
    
    # Enregistrer les logs des projets
    for entry in log_entries:
        log_project_info(entry['project_name'], entry['status'], entry['duration'])
        time.sleep(1)  # Simule une attente entre les logs
    
    # Liste tous les dossiers dans le répertoire principal
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
    
    # Lire chaque fichier de log et écrire dans le CSV en mode append
    for log_file in log_file_paths:
        # Lire le fichier de log
        log_df = pd.read_csv(log_file, names=['date', 'levelname', 'project_name', 'status', 'duration'])
        
        # Écrire dans un fichier CSV en mode append
        log_df.to_csv(csv_file_path, mode='a', header=not os.path.exists(csv_file_path), index=False)
    
    # Nettoyer le fichier de log temporaire
    if os.path.exists(temp_log_file_path):
        os.remove(temp_log_file_path)
    
log_directory = '/home/support-info/TM/'
csv_file_path = '/home/support-info/TM/09-monitoring-global/log.csv'
duration = '2024-07-24 14:41:25'
generate_log_and_write_csv(log_directory, csv_file_path, duration)





        
