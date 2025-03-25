import pandas as pd
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

# Initialize classes
db_connector = DatabaseConnector()
data_extractor = DataExtractor()
data_cleaning = DataCleaning()

# Step 1: Connect to RDS database and extract table list
engine_rds = db_connector.init_db_engine(db_type='RDS')
tables = db_connector.list_db_tables(engine_rds)
print("Available tables:", tables)

# Step 2: Extract data from the user data table
user_data = data_extractor.read_rds_table(db_connector, 'legacy_users')

# Step 3: Clean user data
cleaned_data = data_cleaning.clean_user_data(user_data)

# Step 4: Extract data from the card data table
card_data = data_extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')

# Step 5: Clean card data
cleaned_card_data = data_cleaning.cleaned_card_data(card_data)

# Step 6: Upload cleaned data to the local 'sales_data' database
engine_local = db_connector.init_db_engine(db_type='LOCAL')

try:
    db_connector.upload_to_db(cleaned_data, 'dim_users')
    print(" Successfully uploaded cleaned user data to 'dim_users'.")
except Exception as e:
    print(f"Error uploading user data: {e}")

try:
    db_connector.upload_to_db(cleaned_card_data, 'dim_card_details')
    print("Successfully uploaded cleaned card data to 'dim_card_details'.")
except Exception as e:
    print(f"Error uploading card data: {e}")
