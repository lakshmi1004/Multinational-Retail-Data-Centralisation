import pandas as pd
import logging
import numpy as np
import re

logger = logging.getLogger(__name__)


class DataCleaning:
    
    def clean_user_data(self, df):
        # 1. Change "NULL" string values to actual NULL values (NaN)
        df.replace("NULL", pd.NA, inplace=True)

        # 2. Drop rows with NULL values in critical columns
        df.dropna(subset=['first_name', 'last_name', 'email_address', 'date_of_birth', 'join_date'], inplace=True)

        # 3. Convert 'date_of_birth' and 'join_date' to datetime using format='mixed'
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], format='mixed', errors='coerce')
        df['join_date'] = pd.to_datetime(df['join_date'], format='mixed', errors='coerce')

        # 4. Identify and fix any remaining invalid dates
        invalid_dates = df['join_date'].isna()
        if invalid_dates.sum() > 0:
            print(f"Found {invalid_dates.sum()} rows with date issues — attempting manual correction.")
            df.loc[invalid_dates, 'join_date'] = df.loc[invalid_dates, 'join_date'].apply(self.fix_date_format)

        # 5. Remove remaining NaT values after correction attempts
        df.dropna(subset=['date_of_birth', 'join_date'], inplace=True)

        # 6. Correct data types
        df['phone_number'] = df['phone_number'].astype(str)

        # 7. Remove duplicate records
        df.drop_duplicates(inplace=True)
        
        # 8. Final row count check
        if len(df) == 15284:
            print("Data cleaning successful. 15284 rows retained.")
        else:
            print(f"Warning: Cleaned data has {len(df)} rows instead of 15284.")
        
        return df

    # Backup method for correcting tricky date formats
    def fix_date_format(self, date_str):
        """Attempts to manually fix non-standard date formats."""
        try:
            return pd.to_datetime(date_str, dayfirst=True, errors='coerce')
        except:
            return pd.NaT

    def clean_card_data(self, df):
            """
            Cleans the card data by handling missing values, formatting issues, and erroneous data.
            
            :param df: DataFrame containing raw card details
            :return: Cleaned DataFrame
            """
            df = df.copy()  # Avoids SettingWithCopyWarning
            print(f"Initial row count: {df.shape[0]}")
            #1. Normalize column names
            #df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
            #print(f"Row count after normalising: {df.shape[0]}")
        
            # 2. Convert "NULL" string values to NaN
            df.replace("NULL", pd.NA, inplace=True)
            
            # Print missing values BEFORE dropping them
            print("Missing values before cleaning:\n", df.isna().sum())

            # 3.  **Remove NULL values**
            required_columns = ['card_number', 'expiry_date', 'card_provider', 'date_payment_confirmed']
            df.dropna(subset=required_columns, inplace=True)
            print(f"After dropping NULL values: {df.shape[0]}")

            # 4. Remove duplicate card numbers
            df.drop_duplicates(subset=['card_number'], keep='first', inplace=True)
            print(f"After removing duplicate card numbers: {df.shape[0]}")

            # 5. Keep only numeric card numbers
            df['card_number'] = df['card_number'].astype(str)  # Ensure it's a string
            df['clean_card_number'] = df['card_number'].str.extract(r'(\d+)')  # Extract only numbers

            df = df[df['clean_card_number'].astype(str).str.isnumeric()]
            print(f"After removing non-numeric card numbers: {df.shape[0]}")

            def convert_payment_date(date_str):
                """Try multiple date formats for date_payment_confirmed."""
                date_formats = ["%d-%m-%Y", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y %m %d","%Y/%m/%d",
                  "%Y %d %m", "%m %d %Y", "%d %m %Y","%m %Y %d" , "%m %Y %d", "%d %Y %m", "%Y.%m.%d", "%Y.%d.%m",
                  "%m.%d.%Y", "%d.%m.%Y", "%Y.%m.%d", "%Y.%d.%m", "%m.%d.%Y",
                  "%Y.%m.%d", "%Y.%d.%m", "%m.%d.%Y", "%d.%m.%Y", "%Y.%m.%d", "%Y.%d.%m", "%m.%d.%Y",
                    "%d.%m.%Y", "%Y%m%d", "%Y%d%m", "%m%d%Y", "%d%m%Y","%Y %m", "%Y %d", "%m %Y", "%d %Y",
                    "%Y/%m", "%Y %m", "%m %Y", "%d %Y", "%Y.%m", "%Y.%d", "%m.%Y", "%d.%Y", "%Y%m", "%Y %m", "%m %Y",
                    "%d %Y", "%B %Y %d", "%B %d %Y", "%Y %B %d", "%Y %d %B", "%d %B %Y", "%B %Y %d", "%B %d %Y",
                    "%Y %B", "%Y %d", "%B %Y", "%d %Y", "%B %Y", "%Y %B", "%B %Y", "%d %B", "%B %d", "%Y %B", "%B %Y"]             
                
                for fmt in date_formats:
                    try:
                        return pd.to_datetime(date_str, format=fmt)
                    except (ValueError, TypeError):
                        continue
                return pd.NaT  # Return NaT if no formats match

                # Apply the function to the column
            df["date_payment_confirmed"] = df["date_payment_confirmed"].apply(convert_payment_date)

            # Check which rows still have unparsed dates (optional debug step)
            invalid_dates = df[df["date_payment_confirmed"].isna()]
            print(f"Unparsed dates: {len(invalid_dates)}")
           # print(f"Unparsed dates: {invalid_dates}")

            # Drop rows where date conversion failed
            df.dropna(subset=["date_payment_confirmed"], inplace=True)
            print(f"After converting 'date_payment_confirmed': {df.shape[0]}")
            

            # 6. Final row count check
            final_row_count = df.shape[0]
            removed_rows = df.shape[0] - final_row_count
            print(f"Rows removed during cleaning: {removed_rows}")
            if final_row_count != 15284:
                print(f"Warning: Expected 15,284 rows but got {final_row_count}")
         
            return df
          
    def clean_store_data(self, df):
        """
        Cleans the store details DataFrame by handling NULL values, converting data types,
        and formatting the 'staff_numbers' column correctly.

        Args:
            df (pd.DataFrame): The store details DataFrame.

        Returns:
            pd.DataFrame: The cleaned DataFrame.
        """
        df = df.copy()
        print(f"Initial row count: {df.shape[0]}")

        if 'lat' in df.columns:
            df.drop(columns=['lat'], inplace=True) # Remove the 'lat' column, as it seems to be an empty duplicate of 'latitude'
            print(f"dropped lat column")

         # ✅ Validate 'store_code' format (First 2-3 letters, hyphen, 8 alphanumeric characters)
        store_code_pattern = re.compile(r'^[A-Za-z]{2,3}-[A-Za-z0-9]{8}$')
        df = df[df['store_code'].astype(str).str.match(store_code_pattern, na=False)]
 
        #Convert 'opening_date' to datetime format (handle different formats)
        def date(date_str):
                """Try multiple date formats for date_payment_confirmed."""
                date_formats = ["%d-%m-%Y", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y %m %d","%Y/%m/%d",
                  "%Y %d %m", "%m %d %Y", "%d %m %Y","%m %Y %d" , "%m %Y %d", "%d %Y %m", "%Y.%m.%d", "%Y.%d.%m",
                  "%m.%d.%Y", "%d.%m.%Y", "%Y.%m.%d", "%Y.%d.%m", "%m.%d.%Y",
                  "%Y.%m.%d", "%Y.%d.%m", "%m.%d.%Y", "%d.%m.%Y", "%Y.%m.%d", "%Y.%d.%m", "%m.%d.%Y",
                    "%d.%m.%Y", "%Y%m%d", "%Y%d%m", "%m%d%Y", "%d%m%Y","%Y %m", "%Y %d", "%m %Y", "%d %Y",
                    "%Y/%m", "%Y %m", "%m %Y", "%d %Y", "%Y.%m", "%Y.%d", "%m.%Y", "%d.%Y", "%Y%m", "%Y %m", "%m %Y",
                    "%d %Y", "%B %Y %d", "%B %d %Y", "%Y %B %d", "%Y %d %B", "%d %B %Y", "%B %Y %d", "%B %d %Y",
                    "%Y %B", "%Y %d", "%B %Y", "%d %Y", "%B %Y", "%Y %B", "%B %Y", "%d %B", "%B %d", "%Y %B", "%B %Y"]             
                
                for fmt in date_formats:
                    try:
                        return pd.to_datetime(date_str, format=fmt)
                    except (ValueError, TypeError):
                        continue
                return pd.NaT  # Return NaT if no formats match
        
        df['opening_date'] =df['opening_date'].apply(date)

       #Remove non-numeric characters from 'staff_number' column
        if 'staff_numbers' in df.columns:
            # Strip non-numeric characters (symbols, letters, spaces) and convert to integers
            df['staff_numbers'] = df['staff_numbers'].astype(str).str.extract(r'(\d+)')
            df['staff_numbers'] = pd.to_numeric(df['staff_numbers'], errors='coerce')
   
        print(f"After removing non-numeric characters: {df.shape[0]}")
          
        initial_row_count = len(df)
        df.dropna(how='all',inplace=True)  # Remove all rows with any NULL values
        final_row_count = len(df)
        dropped_rows = initial_row_count - final_row_count
        print(f"Initial row count: {initial_row_count}")
        print(f"Dropped {dropped_rows} rows due to NULL values.")
        print(f"Final row count: {final_row_count}")

        return df
