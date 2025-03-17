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
engine_local = db_connector.init_db_engine(db_type='LOCAL')

# Step 4: Upload cleaned data to the local 'sales_data' database
db_connector.upload_to_db(cleaned_data, 'dim_users')
