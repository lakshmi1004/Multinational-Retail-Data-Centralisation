import pandas as pd
import logging
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize classes
db_connector = DatabaseConnector()
data_extractor = DataExtractor(api_key='yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX')
data_cleaning = DataCleaning()

# Step 1: Connect to RDS database and extract table list
try:
    engine_rds = db_connector.init_db_engine(db_type='RDS')
    tables = db_connector.list_db_tables(engine_rds)
    logger.info("Available tables: %s", tables)
except Exception as e:
    logger.error("Error connecting to RDS database: %s", e)
    raise

# Step 2: Extract data from the user data table
try:
    user_data = data_extractor.read_rds_table(db_connector, 'legacy_users')
except Exception as e:
    logger.error("Error extracting user data: %s", e)
    raise

# Step 3: Clean user data
try:
    cleaned_user_data = data_cleaning.clean_user_data(user_data)
except Exception as e:
    logger.error("Error cleaning user data: %s", e)
    raise

# Step 4: Extract data from the card data table
try:
    card_data = data_extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
except Exception as e:
    logger.error("Error extracting card data: %s", e)
    raise

# Step 5: Clean card data
try:
    cleaned_card_data = data_cleaning.clean_card_data(card_data)
except Exception as e:
    logger.error("Error cleaning card data: %s", e)
    raise

# Step 6: Get the number of stores
try:
    number_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    number_of_stores = data_extractor.list_number_of_stores(number_stores_endpoint)
except Exception as e:
    logger.error("Error retrieving number of stores: %s", e)
    raise

# Step 7: Retrieve store data
try:
    store_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{}"
    stores_df = data_extractor.retrieve_stores_data(store_endpoint, number_of_stores)

    if stores_df.empty:
        logger.warning("No store data retrieved. Check API response.")
    else:
        logger.info("Successfully retrieved store data.")

    # Log the columns of the DataFrame to inspect the structure
    logger.info(f"Columns in stores_df: {stores_df.columns}")

    # Step 8: Clean the store data
    cleaned_stores_data = data_cleaning.clean_store_data(stores_df)

except Exception as e:
    logger.error("Error cleaning store data: %s", e)
    raise


# Step 9: Upload cleaned data to the local 'sales_data' database
engine_local = db_connector.init_db_engine(db_type='LOCAL')

# Step 9.1: Function to upload data to DB
def upload_data_to_db(data, table_name):
    try:
        db_connector.upload_to_db(cleaned_user_data, 'dim_users')  # Only pass the cleaned data and the table name
        print("Successfully uploaded cleaned user data to 'dim_users'.")
    except Exception as e:
        print(f"Error uploading user data: {e}")

try:
    db_connector.upload_to_db(cleaned_card_data, 'dim_card_details')  # Only pass the cleaned data and the table name
    print("Successfully uploaded cleaned card data to 'dim_card_details'.")
except Exception as e:
    print(f"Error uploading card data: {e}")

try:
    db_connector.upload_to_db(cleaned_stores_data, 'dim_store_details')  # Only pass the cleaned data and the table name
    print("Successfully uploaded cleaned store data to 'dim_store_details'.")
except Exception as e:
    print(f"Error uploading store data: {e}")

