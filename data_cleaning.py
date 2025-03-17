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
