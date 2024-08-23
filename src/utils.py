
import pymssql
from pyhocon import ConfigFactory
import logging
import pandas as pd 
from datetime import datetime
import sys
import os 
import shutil
from pyhocon import ConfigFactory
from decorators.timing import get_time
import re
import streamlit as st
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import time
from decorators.timing import get_time
import matplotlib.pyplot as plt
import tempfile
from fpdf import FPDF
from matplotlib.backends.backend_pdf import PdfPages
from io import BytesIO


# Get config file 
config_file = 'src/config.conf'


# Initialize the logger
logger = logging.getLogger(__name__)

# Create the "logs" directory if it doesn't exist
if not os.path.exists('logs'):
    os.makedirs('logs')
# Set up the logger
# Set up the logger
current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
project_name = os.path.basename(os.getcwd())
log_filename = f'logs/log_{current_time}_{project_name}.log'
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Check if the config file exists
if not os.path.isfile(config_file):
    logger.error(f'Config file not found: {config_file}')
    sys.exit()

# Check if the script has read permission to access the file
if not os.access(config_file, os.R_OK):
    logger.error(f'Permission denied to read config file: {config_file}')
    sys.exit()




config = ConfigFactory.parse_file(config_file) 
# Get back our configuration 
def getting_configuration_back():
    

    server = config.db_prod.server
    database = config.db_prod.database
    username = config.db_prod.username
    password = config.db_prod.password
    table_name = config.db_prod.table_name
    column_dtypes = config.dataframes.column_dtypes
    columns_to_convert = config.dataframes.columns_to_convert
    date_columns = config.dataframes.date_columns

    
    return server, database, username, password, table_name, column_dtypes, columns_to_convert, date_columns
        
server, database, username, password, table_name, column_dtypes, columns_to_convert, date_columns = getting_configuration_back()


def build_sql_pymssql(server, database, username, password):
    try:
        conn = pymssql.connect(host=rf'{server}', user=rf'{username}', password=rf'{password}', database=rf'{database}')
        cur = conn.cursor()
        logger.info("Successfully connected to the database.")
        return conn, cur
    except pymssql.Error as e:
        logger.error(f"An error occurred while connecting to the database: {e}")
        raise e

@get_time
def insert_data_into_db(df: pd.DataFrame, table_name_with_schema: str, server: str, database: str, username: str, password: str):
    """
    Inserts data from a pandas DataFrame into an existing SQL Server table if condition is set to False else creating a new table.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        The DataFrame containing the data to be inserted into the database table.
    table_name_with_schema : str
        The name of the table with schema in which data will be inserted (e.g., "schema_name.table_name").
    server : str
        The SQL Server instance to connect to.
    database : str
        The name of the database where the table resides.
    username : str
        The username for authenticating with the SQL Server.
    password : str
        The password for authenticating with the SQL Server.

    Returns:
    --------
    None

    Raises:
    -------
    pymssql.Error
        If an error occurs while inserting data into the database.

    Example:
    --------
    import pandas as pd

    # Sample DataFrame
    data = {
        'Column1': [1, 2, 3],
        'Column2': ['A', 'B', 'C']
    }
    df = pd.DataFrame(data)

    # Insert data into the database table
    insert_data_into_db(df, 'schema_name.table_name', 'server_name', 'database_name', 'username', 'password')
    """
    conn, cur = build_sql_pymssql(server, database, username, password)
    
    try:
        schema, table = table_name_with_schema.split('.')
        logger.info(f"Schema: {schema}, Table: {table}")
        # Create schema if it does not exist
        create_schema_query = f"""
        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema}')
        BEGIN
            EXEC('CREATE SCHEMA {schema}')
        END
        """
        cur.execute(create_schema_query)
        logger.info(f"Create schema query: {create_schema_query}")
        df[columns_to_convert] = df[columns_to_convert].astype(float)
        df = df.astype(column_dtypes)
        # Create the table query
        create_table_query = f"""
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}')
        BEGIN
            CREATE TABLE {table_name_with_schema} (
        """
        for column, dtype in zip(df.columns, df.dtypes):
            if dtype == "object":
                create_table_query += f"{column} NVARCHAR(250), "
            elif dtype == "int64":
                create_table_query += f"{column} INT, "
            elif dtype == "float64":
                create_table_query += f"{column} FLOAT, "
            elif dtype == "datetime64[ns]":
                create_table_query += f"{column} DATETIME, "
            else:
                create_table_query += f"{column} NVARCHAR(250), "

        create_table_query = create_table_query[:-2] + ") END"

        # Execute the query to create the table
        cur.execute(create_table_query)
        logger.info(f"Create table query: {create_table_query}")
        # Check the length of columns before inserting, it should be 23
        if len(df.columns) != 7:
            logger.error(f"The length of the DataFrame should be 7, but we have {len(df.columns)}")
        else:
            # Insert data
            columns = ','.join(df.columns)
            placeholders = ','.join(['%s'] * len(df.columns))
            insert_query = f"INSERT INTO {table_name_with_schema} ({columns}) VALUES ({placeholders})"

            values = [tuple(row) for _, row in df.iterrows()]
            cur.executemany(insert_query, values)
            
            # Commit the changes
            conn.commit()
            logger.info("Table updated")
            logger.info(f"Loaded time at {datetime.now()}")
            logger.info(f"Length of DataFrame inserted {len(df)}")
            
    except pymssql.Error as e:
        logger.error(f"An error occurred while inserting data into the database: {e}")
        raise e
    finally:
        cur.close()
        conn.close()
        
        
        
def diff_time(start, end):
    diff = end - start
    days, seconds = diff.days, diff.seconds
    hours = days * 24 + seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return (hours, minutes, seconds)


def clear_cache():
    st.cache_data.clear()
    st.cache_resource.clear()
    
    
def normalize_column_names(df):
    """
    Normalize column names by removing accents and converting to lowercase.
    """
    accented_characters = {'é': 'e', 'ô': 'o', 'à': 'a', 'û': 'u', 'î': 'i'}
    normalized_columns = []
    for col in df.columns:
        normalized_col = ''
        for char in col:
            normalized_char = accented_characters.get(char, char)
            normalized_col += normalized_char
        normalized_col = normalized_col.replace(' ', '_')
        normalized_col = normalized_col.replace("'", '_')
        normalized_columns.append(normalized_col.lower())
    return normalized_columns




@st.cache_data
def get_data(server, database, username, password, sql_file_path) -> pd.DataFrame:
    """
    This function will return pd.dataframe
    
    """
    conn, _ = build_sql_pymssql(server, database, username, password)
    
    try :
        query = read_sql_file(sql_file_path)
        
        result = pd.read_sql(query, conn)
        
    except (pymssql.OperationalError, pymssql.ProgrammingError, pymssql.DatabaseError, pymssql._mssql.MSSQLDatabaseException) as e:
        logger.error(f"An error occurred while reading query into the database: {e}")
        raise e
    finally:
        conn.close()
    
    return result

def generate_report(df):
    # CSV Report
    csv = df.to_csv(index=False)
    st.download_button(
        label="Download data as CSV",
        data=csv,
        file_name='sales_data.csv',
        mime='text/csv',
    )
    

# Send Mail
def send_email_with_attachment(subject, body, attachment_paths, recipient_list):
    # Load the configuration file
    config = ConfigFactory.parse_file('src/config.conf')

    # Define the email parameters
    smtp_server = config.email.smtp_server
    smtp_port = config.get_int('email.smtp_port')
    sender_email = config.email.sender_email

    # Create the message object
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ', '.join(recipient_list)
    msg['Subject'] = subject

    # Attach the message body
    msg.attach(MIMEText(body))

    # Convert my log file into txt 
    def convert_log_to_txt(log_path):
        txt_path = log_path.replace('.log', '.txt')
        with open(log_path, 'r') as log_file, open(txt_path, 'w') as txt_file:
            txt_file.write(log_file.read())
        return txt_path
    # Attach the files
    for attachment_path in attachment_paths:
        with open(attachment_path, 'rb') as f:
            if attachment_path.endswith('.log'):
                attachment_path = convert_log_to_txt(attachment_path)
                attachment = MIMEApplication(f.read(), _subtype='txt')
            elif attachment_path.endswith('.xlsx'):
                attachment = MIMEApplication(f.read(), _subtype='vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            elif attachment_path.endswith('.pdf'):
                attachment = MIMEApplication(f.read(), _subtype='pdf')
            else:
                continue  # Skip unsupported file types
            
            attachment.add_header('Content-Disposition', 'attachment', filename=attachment_path.split('/')[-1])
            msg.attach(attachment)

    # Connect to the SMTP server and send the message
    try:
        # Connect to the SMTP server without authentication
        server = smtplib.SMTP(smtp_server, smtp_port)
        if smtp_port in [587, 25]:  # If using port 587 or 25, start TLS if needed
            server.starttls()
        server.sendmail(sender_email, recipient_list, msg.as_string())
        server.quit()
        logger.info("Email sent successfully")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        
    
@get_time
def read_sql_file(file_path: str) -> str:
    with open(file_path, 'r') as file:
        query = file.read()
        logger.info(f"Query {file} has been opened successfully!")
    return query

@get_time
def read_sql_file(file_path: str) -> str:
    with open(file_path, 'r') as file:
        query = file.read()
        logger.info(f"Query {file} has been opened successfully!")
    return query

@get_time
def drop_data_two_months(server, database, username, password, table_name_with_schema):
    
    _, table = table_name_with_schema.split('.')
    conn, cur = build_sql_pymssql(server, database, username, password)
    delete_query = f"""
    DELETE FROM {table}
    WHERE YearGL = YEAR(DATEADD(MONTH, DATEDIFF(MONTH, -1, getdate())-1, -1))
    AND MonthGL >= MONTH(DATEADD(MONTH, DATEDIFF(MONTH, -1, getdate())-1, -1))
    """
    try:
        cur.execute(delete_query)
        conn.commit()
        
    except pymssql.Error as e:
        logger.error(f"An error occurred while droping data into the database: {e}")
        raise e
    finally:
        cur.close()
        conn.close()
    
@get_time    
def running_queries(server, database, username, password, sql_file_path) -> pd.DataFrame:
    """
    This function will return pd.dataframe
    
    """
    conn, _ = build_sql_pymssql(server, database, username, password)
    
    try :
        query = read_sql_file(sql_file_path)
        
        result = pd.read_sql(query, conn)
        result['load_file'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        result = result.fillna('None')
        result[date_columns] = result[date_columns].applymap(lambda x: pd.to_datetime(x) if pd.notnull(x) else None)
        result = result.astype(column_dtypes)

        
    except (pymssql.OperationalError, pymssql.ProgrammingError, pymssql.DatabaseError, pymssql._mssql.MSSQLDatabaseException) as e:
        logger.error(f"An error occurred while reading query into the database: {e}")
        raise e
    finally:
        conn.close()
    
    return result

@st.cache_data
def get_data(server, database, username, password, sql_file_path) -> pd.DataFrame:
    """
    This function will return pd.dataframe
    
    """
    conn, _ = build_sql_pymssql(server, database, username, password)
    
    try :
        query = read_sql_file(sql_file_path)
        
        result = pd.read_sql(query, conn)
        
    except (pymssql.OperationalError, pymssql.ProgrammingError, pymssql.DatabaseError, pymssql._mssql.MSSQLDatabaseException) as e:
        logger.error(f"An error occurred while reading query into the database: {e}")
        raise e
    finally:
        conn.close()
    
    return result

def generate_csv_report(df):
    csv = df.to_csv(index=False)
    st.download_button(
        label="Download data as CSV",
        data=csv,
        file_name='project_status_data.csv',
        mime='text/csv',
    )


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
    
@get_time
def drop_data(server, database, username, password, table_name_with_schema):
    
    _, table = table_name_with_schema.split('.')
    conn, cur = build_sql_pymssql(server, database, username, password)
    delete_query = f"""
    DROP TABLE IF EXISTS {table_name_with_schema}
    """
    try:
        cur.execute(delete_query)
        conn.commit()
        logger.info("Data are been deleted")
        
    except pymssql.Error as e:
        logger.error(f"An error occurred while droping data into the database: {e}")
        raise e
    finally:
        cur.close()
        conn.close()
        
def generate_pdf_report(df):
    # Create a pivot table from the DataFrame
    df = df.sort_values(by='date', ascending=True)
    pivot_table = df.pivot_table(
        index=['project_name', 'status', 'message'],
        values='duration',
        aggfunc='sum'
    ).reset_index()

    # Streamlit button for generating PDF report
    if st.button("Generate PDF Report"):
        # Get the current date
        current_date = datetime.now().strftime('%Y-%m-%d')
        # Set the title with the current date
        title = f"Project Status Report - {current_date}"

        # Define the number of rows per page
        rows_per_page = 20  # Adjust this based on the size of your data and page format

        # Split the pivot table into chunks
        chunks = [pivot_table.iloc[i:i + rows_per_page] for i in range(0, len(pivot_table), rows_per_page)]

        # Create a BytesIO object to hold the PDF data
        
        pdf_output = BytesIO()

        # Save the plot to a PDF
        with PdfPages(pdf_output) as pdf:
            for chunk in chunks:
                fig, ax = plt.subplots(figsize=(12, 8))  # Increased height for title
                fig.suptitle(title, fontsize=16)  # Add title to the figure
                ax.axis('tight')
                ax.axis('off')
                
                # Create the table for the current chunk
                table = ax.table(cellText=chunk.values, colLabels=chunk.columns, cellLoc='center', loc='center')
                table.auto_set_font_size(False)
                table.set_fontsize(10)
                table.scale(1.2, 1.2)
                
                # Adjust column widths
                column_widths = [1, 1, 3, 1]  # 'message' column width is 3 times others
                for i, width in enumerate(column_widths):
                    table.auto_set_column_width([i])
                    for j in range(len(chunk) + 1):  # +1 for header row
                        table[(j, i)].set_width(width * 0.2)  # Adjust scale factor as needed
                
                pdf.savefig(fig, bbox_inches='tight')
                plt.close(fig)
        
        pdf_output.seek(0)  # Reset the pointer to the beginning of the BytesIO object

        st.download_button(
            label="Download PDF Report",
            data=pdf_output,
            file_name=f'project_status_report_{current_date}.pdf',
            mime='application/pdf',
        )


def generate_pdf_report_from_df_(df, output_path):
    # Create a pivot table from the DataFrame
    df = df.sort_values(by='date', ascending=True)
    pivot_table = df.pivot_table(
        index=['project_name', 'status', 'message'],
        values='duration',
        aggfunc='sum'
    ).reset_index()

    # Get the current date
    current_date = datetime.now().strftime('%Y-%m-%d')
    # Set the title with the current date
    title = f"Project Status Report - {current_date}"

    # Define the number of rows per page
    rows_per_page = 20  # Adjust this based on the size of your data and page format

    # Split the pivot table into chunks
    chunks = [pivot_table.iloc[i:i + rows_per_page] for i in range(0, len(pivot_table), rows_per_page)]

    # Save the plot to a PDF
    with PdfPages(output_path) as pdf:
        for chunk in chunks:
            fig, ax = plt.subplots(figsize=(12, 8))  # Increased height for title
            fig.suptitle(title, fontsize=16)  # Add title to the figure
            ax.axis('tight')
            ax.axis('off')
            
            # Create the table for the current chunk
            table = ax.table(cellText=chunk.values, colLabels=chunk.columns, cellLoc='center', loc='center')
            table.auto_set_font_size(False)
            table.set_fontsize(10)
            table.scale(1.2, 1.2)
            
            # Adjust column widths
            column_widths = [1, 1, 3, 1]  # 'message' column width is 3 times others
            for i, width in enumerate(column_widths):
                table.auto_set_column_width([i])
                for j in range(len(chunk) + 1):  # +1 for header row
                    table[(j, i)].set_width(width * 0.2)  # Adjust scale factor as needed
            
            pdf.savefig(fig, bbox_inches='tight')
            plt.close(fig)
            
# Fonction pour obtenir le statut en fonction du niveau de log
def get_status(level):
    if level == 'INFO':
        return 'success'
    elif level == 'ERROR':
        return 'failed'
    elif level == 'WARNING':
        return 'warning'
    elif level == 'WARN':
        return 'warning'
    return 'unknown'


# Fonction pour ajouter des logs en fonction du message reçu
def log_message(logger, timestamp, message, level, start_time, project_name):
    current_time = datetime.now()
    duration_seconds = (current_time - start_time).total_seconds()
    date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
    tm_index = path_parts.index('TM') + 1
    
    # Le nom du projet se situe après le sous-dossier dans 'TM/<directory>/<sub_directory>/logs/<log_file>'
    project_name = path_parts[tm_index + 1]
    return project_name

def reset_csv_file(file_path):
    # Lire le fichier CSV pour obtenir les colonnes
    df = pd.read_csv(file_path)
    
    # Créer un DataFrame vide avec les mêmes colonnes
    df_vide = pd.DataFrame(columns=df.columns)
    
    # Écrire le DataFrame vide dans le fichier CSV, écrasant le contenu existant
    df_vide.to_csv(file_path, index=False)
    
def process_logs(file_path):
    # Lire le fichier CSV
    df = pd.read_csv(file_path)
    
    # Convertir les colonnes appropriées en types de données corrects
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
    
    # Supprimer la colonne 'date_only'
    df = df.drop(columns='date_only')
    
    return df

def process_log_files(log_directory, logger, start_time):
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