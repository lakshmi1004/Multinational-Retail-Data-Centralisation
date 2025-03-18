import yaml
from sqlalchemy import create_engine
from sqlalchemy import text


class DatabaseConnector:
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as file:
            creds = yaml.safe_load(file)
        return creds

    def init_db_engine(self, db_type='RDS'):
        creds = self.read_db_creds()
        
        if db_type == 'RDS':
            return create_engine(
                f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@"
                f"{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
            )
        elif db_type == 'LOCAL':
            return create_engine(
                f"postgresql://{creds['LOCAL_DB_USER']}:{creds['LOCAL_DB_PASSWORD']}@"
                f"{creds['LOCAL_DB_HOST']}:{creds['LOCAL_DB_PORT']}/{creds['LOCAL_DB_DATABASE']}"
            )


    def list_db_tables(self, engine):
        with engine.connect() as connection:
            result = connection.execute(
                text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            )
            tables = [row[0] for row in result]
        return tables



    def upload_to_db(self, df, table_name):
    # Connect to the LOCAL PostgreSQL database (not the read-only RDS)
        
        creds = self.read_db_creds()

        engine = create_engine(
                f"postgresql://{creds['LOCAL_DB_USER']}:{creds['LOCAL_DB_PASSWORD']}@"
                f"{creds['LOCAL_DB_HOST']}:{creds['LOCAL_DB_PORT']}/{creds['LOCAL_DB_DATABASE']}"
        )

        try:
            with engine.connect() as conn:

                df.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f"Successfully uploaded data to table '{table_name}' in 'sales_data' database.")
        except Exception as e:
            print(f"Error uploading data: {e}")

