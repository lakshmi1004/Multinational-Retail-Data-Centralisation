import pandas as pd

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
   
    def cleaned_card_data(self, df):
        """
        Cleans the card data by handling missing values, formatting issues, and erroneous data.
        
        :param df: DataFrame containing raw card details
        :return: Cleaned DataFrame
        """
        # Step 1: Make an explicit copy to avoid SettingWithCopyWarning
        df = df.copy()

        # Step 2: Initial row count for debugging
        initial_row_count = df.shape[0]
        print('Initial row count:', initial_row_count)

        # Step 3: Strip column names to remove extra spaces
        df.columns = df.columns.str.strip()

        # Step 4: Convert "NULL" strings to actual NaN
        #df.replace("NULL", pd.NA, inplace=True)
        #print('After converting "NULL" strings to actual NaN:', df.shape[0])

        # Step 5: Drop rows with NULL values in required columns
        required_columns = ['card_number', 'expiry_date', 'card_provider', 'date_payment_confirmed']
        df.dropna(subset=required_columns, inplace=True)
        print('After dropping NULL values:', df.shape[0])

        # Step 6: Remove duplicate card numbers
        df.drop_duplicates(subset=['card_number'], keep='first', inplace=True)
        print('After removing duplicate card numbers:', df.shape[0])

        # Step 7: Remove non-numerical card numbers
        df = df[df['card_number'].astype(str).str.isnumeric()].copy()
        print('After removing non-numerical card numbers:', df.shape[0])

        # Step 8: Convert "date_payment_confirmed" to datetime
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce')
        print('After converting "date_payment_confirmed" to datetime:', df.shape[0])

        # Step 9: Check final row count
        final_row_count = df.shape[0]
        removed_rows = initial_row_count - final_row_count
        print(f"Rows removed during cleaning: {removed_rows}")


        print(df[df.isna().any(axis=1)])  # Shows rows with any NULL values before dropping them


        if final_row_count != 15284:
            print(f"⚠ Warning: Expected 15,284 rows but got {final_row_count}")

        return df
