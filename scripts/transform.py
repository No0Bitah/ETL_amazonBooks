import os
import logging
import pandas as pd


# Set up logging
logger = logging.getLogger("etl.transform")

def transform_data(config=None):

    raw_dir = config['raw_data_path']
    processed_dir = config['processed_data_path']
    os.makedirs(processed_dir, exist_ok=True)

    file_path = os.path.join(raw_dir, f"Books.csv")
    
    if not os.path.exists(file_path):
        logger.error(f"Raw data file not found: {file_path}")
        exit()

    logger.info("Starting data transformation process")

    df = pd.read_csv(file_path)

    # Remove duplicates based on 'Title' column
    logger.info("Removing duplicates based on 'Title' column")
    df.drop_duplicates(subset="Title", inplace=True)
    logger.info("Done ...")
    

    # Create a numeric Reviews column
    logger.info("Converting revies from STR to INT")
    df['Reviews'] = df['Reviews'].apply(convert_reviews)
    logger.info("Done ...")


    # clean ratings column and convert string into float
    logger.info("Cleaning ratings column and converting string into float")
    df['Rating'] = df['Rating'].apply(strip_rating)
    logger.info("Done ...")


    # clean price column and convert string into float
    logger.info("Cleaning ratings column and converting string into float")
    df['Price ($)'] = df['Price'].apply(strip_price)
    logger.info("Done ...")

    # Clean column names for PostgreSQL compatibility
    df.columns = [col.replace('/', '_').replace(' ', '_').lower() for col in df.columns]

    # Drop original price column to avoid duplicate columns
    df = df.drop(columns=["Price"])

    top_10 = df.sort_values(by='Reviews', ascending=False).head(10)

    # Add rank column based on reviews (descending order)
    top_10['Rank'] = top_10['Reviews'].rank(method='min', ascending=False).astype(int)

    # Reorder columns to put Rank first
    columns = ['Rank'] + [col for col in top_10.columns if col != 'Rank' and col != 'Reviews']
    top_10 = top_10[columns]

    logger.info(f"Creating top_10_books.csv file")
    file_path_top_10 = os.path.join(processed_dir, f"top_10_books.csv") 
    top_10.to_csv(file_path_top_10, index=False)
    logger.info(f"Books.csv file created in {raw_dir}") 


    logger.info(f"Creating Processed_books.csv file")
    file_path_Processed_books = os.path.join(processed_dir, f"Processed_books.csv") 
    df.to_csv(file_path_Processed_books, index=False)
    logger.info(f"Books.csv file created in {raw_dir}") 


# Function to convert Reviews to integer
def convert_reviews(review_str):
    if pd.isna(review_str):
        return 0
    return int(review_str.replace(',', ''))

# Function to clean ratings column and convert string into float
def strip_rating(ratings):
    if ratings != "N/A":
        return float(ratings.split()[0])

# Function to clean price column and convert string into float
def strip_price(price):
    if price != "N/A":
        return float(price.split("$")[1])
    return 0