import json
import pandas as pd
import snowflake.connector
import logging
from snowflake import snowpark
from snowflake.connector.pandas_tools import write_pandas
from snowflake.snowpark import Session
from snowflake_config import snowflake_config

def ingest_student_data_to_snowflake():
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        # Students JSON file path
        students_file_path = snowflake_config['students_file_path']

        with open(students_file_path) as f:
            data = json.load(f)

        # Use pd.json_normalize to convert the JSON to a DataFrame
        students_df = pd.json_normalize(data['students'], 
                            meta=['student_id', 'name', ['grades', 'math'], ['grades', 'science'], ['grades', 'history'], ['grades', 'english']])

        students_df.columns = ['student_id', 'name', 'math', 'science', 'history', 'english']

        # Missed days JSON file path
        missed_days_file_path = snowflake_config['missed_days_file_path']

        with open(missed_days_file_path) as f:
            data1 = json.load(f)

        # Use pd.json_normalize to convert the JSON to a DataFrame
        missed_days_df = pd.json_normalize(data1['missed_classes'], meta=['student_id', 'missed_days'])

        # Check if 'student_id' and 'missed_days' columns exist in both DataFrames
        if 'student_id' not in students_df.columns or 'student_id' not in missed_days_df.columns:
            raise ValueError("Column 'student_id' is required in both DataFrames.")

        # Merge (join) the DataFrames on 'student_id'
        merged_df = pd.merge(students_df, missed_days_df, on='student_id', how='left')

        #Replace any null values resulted due to left join
        merged_df['missed_days'] = merged_df['missed_days'].fillna(0)

        merged_df = merged_df.astype({'student_id':object,'name':object,'math':int,'science':int, 'history':int, 'english':int, 'missed_days':int })

        # Establish a connection to Snowflake
        conn = snowflake.connector.connect(**snowflake_config)

        # Create a Snowflake table
        table_name = snowflake_config['table_name']
        create_table_query = f"""
        CREATE OR REPLACE TABLE {table_name} (
            student_id STRING,
            name STRING,
            math INT,
            science INT,
            history INT,
            english INT,
            missed_days INT
        )
        """
        conn.cursor().execute(create_table_query)

        merged_df['student_id'] = merged_df['student_id'].astype(str)
        merged_df['name'] = merged_df['name'].astype(str)

        # Create a Snowflake session
        session = snowpark.Session(snowflake_config)

        # Write the Pandas DataFrame to Snowflake table
        write_pandas(conn=session, df=merged_df, table_name=table_name)

        # Query to create the view VW_STUDENTS in STUDENTS_SEMANTIC
        view_query = f"CREATE OR REPLACE VIEW STUDENTS_SEMANTIC.STUDENTS.VW_STUDENTS AS SELECT * FROM STUDENTS_STAGING.STUDENTS_STG.{table_name} WHERE MATH > 90;"

        conn.cursor().execute(view_query)

        # Close the connection
        conn.close()

        logger.info("Data ingestion to Snowflake completed successfully.")

    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")

if __name__ == "__main__":
    ingest_student_data_to_snowflake()
