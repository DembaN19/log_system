import os

log_directory = '/home/support-info/TM/'
all_items = os.listdir(log_directory)
directories = [item for item in all_items if os.path.isdir(os.path.join(log_directory, item))]

log_file_paths = []

# Explorer chaque sous-dossier dans chaque dossier pour trouver les fichiers de log
for directory in directories:
    sub_directory_path = os.path.join(log_directory, directory)
    sub_directories = [item for item in os.listdir(sub_directory_path) if os.path.isdir(os.path.join(sub_directory_path, item))]
    
    for sub_directory in sub_directories:
        logs_directory = os.path.join(sub_directory_path, sub_directory, 'logs')
        
        # Vérifier si le sous-dossier 'logs' existe
        if os.path.isdir(logs_directory):
            log_files = [file for file in os.listdir(logs_directory) if file.endswith('.log')]
            
            # Ajouter les chemins complets des fichiers de log à la liste
            for log_file in log_files:
                full_log_file_path = os.path.join(logs_directory, log_file)
                log_file_paths.append(full_log_file_path)

# Pour visualiser les chemins des fichiers de log trouvés
for path in log_file_paths:
    print(path)
    
    
    
def extract_project_name(log_file_path):
    # Extrait le nom du répertoire parent juste après 'TM/'
    path_parts = log_file_path.split('/')
    tm_index = path_parts.index('TM') + 1
    
    # Le nom du projet se situe après le sous-dossier dans 'TM/<directory>/<sub_directory>/logs/<log_file>'
    project_name = path_parts[tm_index + 1]
    return project_name

# Exemple d'utilisation
log_file_path = '/home/support-info/TM/directory/sub_directory/logs/log_file.log'
print(extract_project_name(log_file_path))