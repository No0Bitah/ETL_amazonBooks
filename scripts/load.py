import os
import logging
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine


# Set up logging
logger = logging.getLogger("etl.load")

def load_data(config=None):

    try:
        processed_dir = config['processed_data_path']
        conn_id = config['postgres_conn_id']
        
        logger.info(f"Starting data loading process to PostgreSQL database using connection: {conn_id}")
        
        # Get PostgreSQL connection using Airflow's PostgresHook
        pg_hook = PostgresHook(postgres_conn_id=conn_id)
        conn = pg_hook.get_conn()
        conn.autocommit = False
        cursor = conn.cursor()
        
        # Create tables if they don't exist
        create_database_schema(cursor)

        # Create SQLAlchemy engine for pandas to_sql
        engine = pg_hook.get_sqlalchemy_engine()

        
        # Dictionary of files to load
        files_to_load = {
            'Processed_books.csv': 'books',
            'top_10_books.csv': 'top_10_books',
        }

        # Process each file
        for file_name, table_name in files_to_load.items():
            file_path = os.path.join(processed_dir, file_name)
            logger.info(f"file path is {file_path}")
            
            if not os.path.exists(file_path):
                logger.warning(f"Processed data file not found: {file_path}")
                continue
                
            logger.info(f"Loading {file_name} into {table_name} table")  
            
            # Read the CSV file
            df = pd.read_csv(file_path)
            logger.info(f"CSV read complete. Shape: {df.shape}")

            pg_hook.run(f"DROP TABLE IF EXISTS {table_name}")

            logger.info(f"Creating {table_name} table and loading {len(df)} rows")

            df.to_sql(
                name=table_name,
                con=engine,
                if_exists='replace',
                index=False
            )

    except Exception as e:
        logger.error(f"Error during data loading: {str(e)}")
        if 'conn' in locals() and conn:
            conn.rollback()
            conn.close()
        logger.error(f"Data loading failed: {str(e)}")       

def create_database_schema(cursor):
    """Create the database schema if it doesn't exist."""
    try:
        # Create books table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS books (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            author TEXT NOT NULL,
            reviews FLOAT,
            rating FLOAT,
            price$ FLOAT
        )
        """)

        # Create top10  table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS books (
            id SERIAL PRIMARY KEY,
            rank INT NOT NULL,
            title TEXT NOT NULL,
            author TEXT NOT NULL,
            reviews FLOAT,
            rating FLOAT,
            price$ FLOAT
        )
        """)
        
        logger.info("Database schema created successfully")
        
    except Exception as e:
        logger.error(f"Error creating database schema: {str(e)}")
        raise