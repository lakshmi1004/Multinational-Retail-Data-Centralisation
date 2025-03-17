import pandas as pd

class DataCleaning:
    def clean_user_data(self, df):
        # Drop rows with null values
        df.dropna(inplace=True)

        # Fix date formatting issues
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce')
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')

        # Remove rows with invalid date entries
        df = df[df['date_of_birth'].notna()]
        df = df[df['join_date'].notna()]

        # Correct data types where necessary
        df['phone_number'] = df['phone_number'].astype(str)

        # Remove duplicate records
        df.drop_duplicates(inplace=True)

        # Final row count check
        if len(df) == 15284:
            print("Data cleaning successful. 15284 rows retained.")
        else:
            print(f"Warning: Cleaned data has {len(df)} rows instead of 15284.")
        
        return df