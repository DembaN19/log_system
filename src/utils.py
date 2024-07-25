
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
import time

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
log_filename = f'logs/log_{current_time}_budget_PWE.log'
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
    

    server = config.db_dwh.server
    database = config.db_dwh.database
    username = config.db_dwh.username
    password = config.db_dwh.password
    table_name = config.db_dwh.table_name
    column_dtypes = config.dataframes.column_dtypes
    columns_to_convert = config.dataframes.columns_to_convert
    date_columns = config.dataframes.date_columns
    columns_to_keep = config.dataframes.columns_to_keep
    
    return server, database, username, password, table_name, column_dtypes, columns_to_convert, date_columns, columns_to_keep
        
server, database, username, password, table_name, column_dtypes, columns_to_convert, date_columns, columns_to_keep = getting_configuration_back()


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
        if len(df.columns) != 26:
            logger.error(f"The length of the DataFrame should be 24, but we have {len(df.columns)}")
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



@get_time       
def move_files_parquet_to_archive(source_folder, archive_folder):
    
    try:
        # Iterate over all files in the source folder
        for file_name in os.listdir(source_folder):
            # Move the file to the archive folder
            src_file_path = os.path.join(source_folder, file_name)
            
            year = datetime.now().strftime("%Y")
            
            month = datetime.now().strftime("%m")
            # Create year folder if it doesn't exist
            year_folder = os.path.join(archive_folder, year)
            if not os.path.exists(year_folder):
                os.makedirs(year_folder)
            # Create month subfolder if it doesn't exist
            month_folder = os.path.join(year_folder, month)
            if not os.path.exists(month_folder):
                os.makedirs(month_folder)
            # Move the file to the month-specific folder
            dest_file_path = os.path.join(month_folder, file_name)
            shutil.move(src_file_path, dest_file_path)
            logger.info(f"File moved from {src_file_path} to {dest_file_path}")
    except Exception as e:
        logger.error(f"Error: {e}")
        

def move_files_csv_to_archive(source_folder, archive_folder):
    
    try:
        # Iterate over all files in the source folder
        for file_name in os.listdir(source_folder):
            # Move the file to the archive folder
            src_file_path = os.path.join(source_folder, file_name)
            # Extract date from the file name
            match = re.search(r'(\d{4}-\d{2})-\d{2}_\d{2}-\d{2}\.csv', file_name)
            if match:
                year_month = match.group(1)
                year, month = year_month.split('-')
                # Create year folder if it doesn't exist
                year_folder = os.path.join(archive_folder, year)
                if not os.path.exists(year_folder):
                    os.makedirs(year_folder)
                # Create month subfolder if it doesn't exist
                month_folder = os.path.join(year_folder, month)
                if not os.path.exists(month_folder):
                    os.makedirs(month_folder)
                # Move the file to the month-specific folder
                dest_file_path = os.path.join(month_folder, file_name)
                shutil.move(src_file_path, dest_file_path)
                logger.info(f"File moved from {src_file_path} to {dest_file_path}")
    except Exception as e:
        logger.error(f"Error: {e}")
        

# Check arch files into folder 
def get_archive_path(file_name, archive_folder):
    match = re.search(r'(\d{4})-(\d{2})-\d{2}_\d{2}-\d{2}\.parquet', file_name)
    if match:
        year = match.group(1)
        month = match.group(2)
        return os.path.join(archive_folder, year, month, file_name)
    return None    

# Read all csv and concat them       
def read_csv_files_and_concat_from_directory(directory, archive_folder):
    # Initialize an empty list to store DataFrames
    dataframes = []

    # Iterate over all files in the specified folder
    for file_name in os.listdir(directory):
        # Check if the file ends with .csv and contains _Rapport_ in its name
        if file_name.endswith('.csv') and 'Rapport_' in file_name:
            # Generate the expected archive path
            arch_file_path = get_archive_path(file_name, archive_folder)
            # Skip if the file already exists in the archive directory
            if arch_file_path and os.path.exists(arch_file_path):
                logger.error(f"File {file_name} already exists in the archive directory. Skipping.")
                continue

            # Extract the date and time from the file name using regular expressions
            match = re.search(r'(\d{4})-(\d{2})-(\d{2})_(\d{2})-(\d{2})\.csv', file_name)
            if match:
                year_fab = match.group(1)
                month_fab = match.group(2)
                day_fab = match.group(3)
                hour_fab = match.group(4)
                minute_fab = match.group(5)
                date = f"{year_fab}-{month_fab}-{day_fab}"
                time = f"{hour_fab}:{minute_fab}"

                file_path = os.path.join(directory, file_name)
                # Read the CSV file into a DataFrame
                try:
                    with open(file_path, 'rb') as f:
                        encoding = chardet.detect(f.read())['encoding']
                    df = pd.read_csv(file_path, encoding=encoding, sep=';')
                    logger.info(f"File {file_name} read successfully.")
                    
                    # Add date and time columns to the DataFrame
                    df['date_file'] = date
                    df['time_file'] = time
                    df['file_name'] = file_name
                    df['annee_fab'] = year_fab
                    df['mois_fab'] = month_fab
                    df['load_file'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    # Append the DataFrame to the list
                    dataframes.append(df)
                except pd.errors.ParserError:
                    # Handle parsing errors if needed
                    logger.error(f"Error parsing file {file_name}. Skipping.")
                    pass
    
    if dataframes:
        # Concatenate all DataFrames into a single DataFrame
        combined_df = pd.concat(dataframes, ignore_index=True)
        # Normalize columns names
        combined_df.columns = normalize_column_names(combined_df)
        # Preprocess columns with comma as decimal separator
        combined_df = combined_df.fillna('None')

        for column in columns_to_convert:
            combined_df[column] = combined_df[column].str.replace(' ', '').str.replace(',', '.').astype(float)
        
        # Convert date columns to datetime
        
        try:
            combined_df[date_columns] = combined_df[date_columns].applymap(lambda x: pd.to_datetime(x) if pd.notnull(x) else None)
            # Handle NaT values by replacing them with None
            combined_df = combined_df.where(pd.notnull(combined_df), None)
            combined_df = combined_df.astype(column_dtypes)
        except ValueError as e:
            logger.error(f"Error when converting date columns:{e}!")
        # Get len of csv readed 
        logger.info(f"Len of dfs founded in {directory} : {len(combined_df)}")
        # Get the number of columns
        logger.info(f"Number of columns of our dataframe compiled : {len(combined_df.columns)}")
        return combined_df
    else:
        logger.error("No valid CSV files found in the directory.")
        return pd.DataFrame()
    
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




def udf_normalize_value(s):
    # To lowercase and trim
    lower_s = s.strip().lower()
    
    # Remove the following chars ! " # % & ' ( ) * + , : ; < = > ? [ ] ^ ` { } ~
    chars_removed = re.sub(r"[!\"#%&'()*+,:;<=>?[\]^`{}~]", '', lower_s)
    
    # Replace multiple spaces with a single space
    trailing_removed = re.sub(r'\s+', ' ', chars_removed)
    
    # Remove accents
    a_rem = re.sub(r'[àáâãäå]', 'a', trailing_removed)
    e_rem = re.sub(r'[èéêë]', 'e', a_rem)
    i_rem = re.sub(r'[ìíîï]', 'i', e_rem)
    o_rem = re.sub(r'[òóôõö]', 'o', i_rem)
    u_rem = re.sub(r'[ùúûü]', 'u', o_rem)
    
    return u_rem.upper()

@get_time 
def read_with_parquet(directory, archive_folder):
    # Initialize an empty list to store DataFrames
    dataframes = []

    # Iterate over all files in the specified folder
    for file_name in os.listdir(directory):
        # Check if the file ends with .parquet
        if file_name.endswith('.parquet'):
            # Generate the expected archive path
            arch_file_path = os.path.join(archive_folder, file_name)
            # Skip if the file already exists in the archive directory
            if os.path.exists(arch_file_path):
                logger.error(f"File {file_name} already exists in the archive directory. Skipping.")
                continue

            file_path = os.path.join(directory, file_name)
            # Read the parquet file into a DataFrame
            try:
                df = pd.read_parquet(file_path, engine='pyarrow')
                logger.info(f"File {file_name} read successfully.")
                
                df['load_file'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                df = df.loc[:, columns_to_keep]
                dataframes.append(df)
            except Exception as e:
                logger.error(f"Error reading file {file_name}: {e}. Skipping.")
                continue

    if dataframes:
        # Concatenate all DataFrames into a single DataFrame
        combined_df = pd.concat(dataframes, ignore_index=True)
        combined_df['load_file'] = pd.to_datetime(combined_df['load_file'])
        combined_df = combined_df.where(pd.notnull(combined_df), None)
        
        combined_df = combined_df.loc[combined_df['Entity'] != None]
        for column in columns_to_convert:
            combined_df[column] = combined_df[column].str.replace(' ', '').str.replace(',', '.').astype(float)
            
        
        
        
        # Normalize column names
        
        
        combined_df = combined_df.fillna('None')
        
        combined_df = combined_df.loc[combined_df['Amount'] != 'None']
        combined_df = combined_df.astype(column_dtypes)
        combined_df.columns = normalize_column_names(combined_df)
        combined_df['amount'] = combined_df['amount'].astype(float)
        combined_df['salespersonname'] = combined_df['salespersonname'].apply(udf_normalize_value)
        
        

        logger.info(f"Len of DataFrames found in {directory}: {len(combined_df)}")
        logger.info(f"Number of columns in the compiled DataFrame: {len(combined_df.columns)}")
        return combined_df
    else:
        logger.error("No valid parquet files found in the directory.")
        return pd.DataFrame()
    
@get_time 
def convert_csv_to_parquet(source_dir: str, target_dir: str) -> None:
    """
    This function will convert CSV files to Parquet format.
    """
    # Get current time for log filename

    # Ensure the target directory exists
    os.makedirs(target_dir, exist_ok=True)

    # Get a list of all CSV files in the source directory
    csv_files = [f for f in os.listdir(source_dir) if f.endswith('.csv') and 'Rapport_' in f]

    logger.info("Conversion has been started!")
    
    # Loop through the CSV files and convert each to Parquet
    for csv_file in csv_files:
        csv_path = os.path.join(source_dir, csv_file)
        parquet_path = os.path.join(target_dir, csv_file.replace('.csv', '.parquet'))

        try:
            # Read the CSV file with better error handling
            df = pd.read_csv(csv_path, encoding='latin-1', sep=";")

            # Convert to Parquet and save
            df.to_parquet(parquet_path, index=False)
            
            if not df.empty:
                logger.info(f"Successfully converted {csv_file} to parquet!")
            else:
                logger.warning(f"The file {csv_file} is empty after processing.")
                
        except pd.errors.ParserError as e:
            logger.error(f"Failed to convert {csv_file}: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred while converting {csv_file}: {e}")
    
    logger.info("Conversion process completed.")
    return None

@get_time 
def convert_excel_to_parquet(source_dir: str, target_dir: str) -> None:
    """
    This function will convert CSV files to Parquet format.
    """
    # Get current time for log filename

    # Ensure the target directory exists
    os.makedirs(target_dir, exist_ok=True)

    # Get a list of all CSV files in the source directory
    excel_files = [f for f in os.listdir(source_dir) if f.endswith('.xlsx') and 'Base' in f or 'CA' in f]

    logger.info("Conversion has been started!")
    
    # Loop through the CSV files and convert each to Parquet
    for excel_file in excel_files:
        excel_path = os.path.join(source_dir, excel_file)
        parquet_path = os.path.join(target_dir, excel_file.replace('.xlsx', '.parquet'))

        try:
            # Read the CSV file with better error handling
            df = pd.read_excel(excel_path, dtype=str)
            df = df.rename(columns={"Name": "CustomerName"})
            # Convert to Parquet and save
            df.to_parquet(parquet_path, index=False)
            
            if not df.empty:
                logger.info(f"Successfully converted {excel_file} to parquet!")
            else:
                logger.warning(f"The file {excel_file} is empty after processing.")
                
        except pd.errors.ParserError as e:
            logger.error(f"Failed to convert {excel_file}: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred while converting {excel_file}: {e}")
    
    logger.info("Conversion process completed.")
    return None

@get_time 
def move_files_excel_to_archive(source_folder, archive_folder):
    
    try:
        # Iterate over all files in the source folder
        for file_name in os.listdir(source_folder):
            # Move the file to the archive folder
            src_file_path = os.path.join(source_folder, file_name)
            # Extract date from the file name
            
            year = datetime.now().strftime("%Y")
            month = datetime.now().strftime("%m")
            # Create year folder if it doesn't exist
            year_folder = os.path.join(archive_folder, year)
            if not os.path.exists(year_folder):
                os.makedirs(year_folder)
            # Create month subfolder if it doesn't exist
            month_folder = os.path.join(year_folder, month)
            if not os.path.exists(month_folder):
                os.makedirs(month_folder)
            # Move the file to the month-specific folder
            dest_file_path = os.path.join(month_folder, file_name)
            shutil.move(src_file_path, dest_file_path)
            logger.info(f"File moved from {src_file_path} to {dest_file_path}")
    except Exception as e:
        logger.error(f"Error: {e}")
        
        
# read adp 

df_adp = pd.read_excel("data/output/ADP SALESPERSONNAME.xlsx")


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
    

def send_email_with_attachment(subject, body, recipient_list):
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

def generate_report(df):
    # CSV Report
    csv = df.to_csv(index=False)
    st.download_button(
        label="Download data as CSV",
        data=csv,
        file_name='sales_data.csv',
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