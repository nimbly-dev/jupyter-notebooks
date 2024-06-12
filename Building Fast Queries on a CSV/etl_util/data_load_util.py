from pyspark.sql import SparkSession, DataFrame, Window
import os
import shutil
from datetime import datetime
import psycopg2
from sqlalchemy import create_engine, inspect
from models import Base


class DataLoadUtil:
    
    def __init__(self):
        """Initialize utility without Spark context."""
        pass

    @staticmethod
    def table_exists(engine, table_name):
        inspector = inspect(engine)
        return table_name in inspector.get_table_names()
    
    @staticmethod
    def create_table_orm(host: str, port: int, dbname: str, user: str, password: str, table_name: str, model_base):
        engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}')
        if not DataLoadUtil.table_exists(engine, table_name):
            model_base.metadata.create_all(engine)
            print(f"Table '{table_name}' created.")
        else:
            print(f"Table '{table_name}' already exists. Skipping creation.")

    # TODO: Seperate this into a new class to avoid inner method
    @staticmethod
    def upsert_data_to_db(df: DataFrame, connection_properties: dict, table_name: str, primary_key_column: str, columns_to_insert: list):
        """
        Upsert data into a PostgreSQL table using a DataFrame.
        
        This function processes each partition of the DataFrame, constructs an SQL upsert statement,
        and executes it using a PostgreSQL connection. The SQL script is constructed to handle
        conflicts by updating existing rows when a conflict on the primary key occurs.

        Src: https://medium.com/@thomaspt748/how-to-upsert-data-into-a-relational-database-using-apache-spark-part-2-python-version-da30352c0ca9
        """
        
        def process_row(row, dbc_merge):
            #row_dict = row.asDict()
            row_dict = row.asDict()
            # Explicitly cast float values to ensure precision and round them to 2 decimal places
            for key, value in row_dict.items():
                if isinstance(value, float):
                    row_dict[key] = round(value, 2)
            columns = ', '.join(columns_to_insert)
            values = ', '.join([f'%({col})s' for col in columns_to_insert])
            update_str = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns_to_insert])

            sql_string = f"""
            INSERT INTO {table_name} ({primary_key_column}, {columns})
            VALUES (%({primary_key_column})s, {values})
            ON CONFLICT ({primary_key_column})
            DO UPDATE SET {update_str};
            """
            dbc_merge.execute(sql_string, row_dict)

        def process_partition(partition):
            db_conn = psycopg2.connect(
                host=connection_properties['host'],
                port=connection_properties['port'],
                user=connection_properties['user'],
                password=connection_properties['password'],
                database=connection_properties['database']
            )
            dbc_merge = db_conn.cursor()

            for row in partition:
                process_row(row, dbc_merge)

            db_conn.commit()
            dbc_merge.close()
            db_conn.close()

        df.foreachPartition(process_partition)
    
    def save_with_timestamp(self, df: DataFrame, directory_path: str):
        """
        Save the DataFrame as a single CSV file with a timestamped name.
        
        Args:
        df (DataFrame): The DataFrame to save.
        directory_path (str): The directory where the CSV file should be saved.
        """
        # Ensure the directory path exists
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
        
        # Define the temporary output directory within the specified directory path
        temp_dir = os.path.join(directory_path, "temp")

        # Repartition the DataFrame to a single partition
        df_single_partition = df.coalesce(1)

        # Save the DataFrame to the temporary directory
        df_single_partition.write.mode("overwrite").option("header", "true").csv(temp_dir)

        # Get the part file from the temporary directory
        part_file = None
        for file in os.listdir(temp_dir):
            if file.startswith("part-") and file.endswith(".csv"):
                part_file = file
                break

        if part_file is None:
            raise FileNotFoundError("Part file not found in the temporary directory.")

        # Generate a timestamp
        #e.g laptops_20240519155641.csv
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

        # Define the final output file path
        final_output_path = os.path.join(directory_path, f"laptops_{timestamp}.csv")

        # Move and rename the part file to the final output path
        shutil.move(os.path.join(temp_dir, part_file), final_output_path)

        # Remove the temporary directory
        shutil.rmtree(temp_dir)

        print(f"File saved as {final_output_path}")

