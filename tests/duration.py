import pandas as pd
from io import StringIO

# Exemple de données sous forme de chaîne avec plusieurs projets
data = """
date,levelname,message,project_name,status,duration,load_file
2024-06-17 15:08:11,INFO,Start timestamp: 2024-06-17 15:08:11.19748,00-streamlit_app,success,0.4032,2024-07-27 11:34:25
2024-06-17 15:08:11,INFO,Len of excel founded in uploaded file 1187,00-streamlit_app,success,0.4035,2024-07-27 11:34:25
2024-06-17 15:09:13,INFO,The processing has been made without any e,00-streamlit_app,success,0.4037,2024-07-27 11:34:25
2024-06-18 10:00:00,INFO,Start timestamp: 2024-06-18 10:00:00.12345,01-another_project,success,0.6000,2024-07-27 11:34:25
2024-06-18 10:01:00,INFO,Processing continued,01-another_project,success,0.6200,2024-07-27 11:34:25
2024-06-19 09:00:00,INFO,Start timestamp: 2024-06-19 09:00:00.54321,02-different_project,success,0.7000,2024-07-27 11:34:25
2024-06-19 09:02:00,INFO,Processing continued,02-different_project,success,0.7100,2024-07-27 11:34:25
2024-06-19 09:05:00,INFO,Final processing,02-different_project,success,0.7200,2024-07-27 11:34:25
"""

# Charger les données dans un DataFrame
df = pd.read_csv(StringIO(data))

# Convertir les colonnes 'date' et 'load_file' en datetime
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

# Afficher le DataFrame mis à jour
df
