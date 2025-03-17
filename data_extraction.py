import pandas as pd
import requests
import boto3
import io

class DataExtractor:
    
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