import pandas as pd
import requests
import boto3
import logging
import tabula
from database_utils import DatabaseConnector

# Initialize logging
logger = logging.getLogger(__name__)

class DataExtractor:

    def __init__(self, api_key):
        self.headers = {'x-api-key': api_key}
            
    def read_rds_table(self, db_connector, table_name):
        engine = db_connector.init_db_engine()
        with engine.connect() as connection:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, connection)
        return df


    def read_csv(self, file_path):
        """Reads data from a CSV file into a pandas DataFrame."""
        try:
            df = pd.read_csv(file_path)
            return df
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            return pd.DataFrame()

    def extract_from_api(self, api_url, headers=None):
        """Extracts data from an API endpoint and returns a pandas DataFrame."""
        try:
            response = requests.get(api_url, headers=headers)
            response.raise_for_status()
            data = response.json()
            return pd.DataFrame(data)
        except Exception as e:
            print(f"Error extracting data from API: {e}")
            return pd.DataFrame()

    def extract_from_s3(self, file_path):
        ''' This method retrieves a table from an S3 bucket and return it as a dataframe.

        This method fetches a table from an S3 bucket using the provided S3 URL, 
        downloads it, and returns the table data as a dataframe. It supports both CSV and JSON formats.

        Args:
            link (`str`): The S3 URL of the table to extract.

        Returns:
            df (`dataframe`): A dataframe containing the table data from the S3 bucket.

        '''

        link_parts = file_path.split('/')

        s3 = boto3.client('s3')
        s3.download_file('data-handling-public', link_parts[-1], link_parts[-1])

        if '.csv' in link_parts[-1]:
            df = pd.read_csv(link_parts[-1])

        elif '.json' in link_parts[-1]:
            df = pd.read_json(link_parts[-1])

        return df
    
    def retrieve_pdf_data(self, card_data):
        """
        Extracts data from a PDF document and returns it as a pandas DataFrame.
        """
        card_data = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        try:
            # Extract data from all pages
            df_list = tabula.read_pdf(card_data, pages='all', multiple_tables=True)
            
            # Combine all extracted tables into one DataFrame
            combined_df = pd.concat(df_list, ignore_index=True)
            
            
            print("PDF data successfully extracted.")
            return combined_df
        
        except Exception as e:
            print(f" Error extracting PDF data: {e}")
            return pd.DataFrame()
        
    def list_number_of_stores(self, number_stores_endpoint,):
        """Returns the number of stores to extract from the API."""
        response = requests.get(number_stores_endpoint, headers=self.headers)
        if response.status_code == 200:
            data = response.json()
            logger.info("Number of stores retrieved: %s", data)
            
            if 'number_stores' in data:
                return data['number_stores']
            else:
                logger.error("The key 'number_stores' was not found in the response data.")
                return None
        else:
            logger.error("Error fetching store number. Response: %s", response.text)
            return None
    def retrieve_stores_data(self, store_endpoint, number_of_stores):
        store_data = []
        failed_stores = []

        for store_number in range(1, number_of_stores + 1):
            try:
                store_url = store_endpoint.format(store_number=store_number)  # Correct formatting
                response = requests.get(store_url, headers=self.headers)
                response.raise_for_status()
                store_data.append(response.json())
            except requests.HTTPError as e:
                logger.error(f"HTTP Error retrieving store {store_number}: {e}")
                failed_stores.append(store_number)

            except requests.RequestException as e:
                logger.error(f"Request Error retrieving store {store_number}: {e}")
                failed_stores.append(store_number)
        return pd.DataFrame(store_data)


    '''def retrieve_stores_data(self, store_endpoint, number_of_stores):
        """
        This method will extract all stores from the API and save them in a pandas DataFrame.
        """
        if number_of_stores is None:
            logger.error("Number of stores is None, cannot retrieve data.")
            return pd.DataFrame()  # Return an empty DataFrame if no stores found
        
        store_data = []
        for store_number in range(1, number_of_stores + 1):
            try:
                store_url = store_endpoint/(store_number)
                response = requests.get(store_url, headers=self.headers)
                response.raise_for_status()
                store_data.append(response.json())
            except requests.HTTPError as e:
                # I log any errors that aren't server (500) errors 
                if e.response.status_code != 500:
                    print(f"An error occurred while retrieving store {store_number}: {e}")
            except requests.RequestException as e:
                print(f"An error occurred while retrieving store {store_number}: {e}")
                # I convert the list of store data into a DataFrame for further processing
                return pd.DataFrame(store_data)'
                '''
    
