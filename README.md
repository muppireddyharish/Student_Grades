Airflow data pipeline to load student data into Snowflake using Pandas.
This repository contains the implementation for loading data from JSON files into Snowflake using Python, pandas, and Apache Airflow. The process involves reading data from JSON files, transforming it using pandas DataFrames, and then loading it into Snowflake tables.
Prerequisites
Following are the prerequisites:
1. Python: Python 3.6.7 or higher installed.
2. Snowflake Connector for Python: Install the pandas-compatible version of the Snowflake Connector for Python using the following command:
pip install "snowflake-connector-python[pandas]"
3. Pandas installed
4. Snowflake account (free account for first 30 days or up to 400$)
5. Snowflake Database, schema are created.
Details
1. Reading Data from the files provided. These files are stored in the local for this project. In a production environment, these are stored in a data lake.
2. Normalize the data to convert the JSON to a dataframe and re-name the columns as needed.
3. Check to make sure we have the common key column to join.
4. Merge/join the students data with missing days data using left outer join. A few students may not have missed any days, so we need to use left join to ensure they are not dropped.
5. Replace nulls that resulted due to left join with zeros 
6. Create Snowflake connection using Snowflake config. Some of the snowflake config details like username, password and account name should be stored in env files, and never be included in the code.
7. Write the final dataframe to Snowflake table STUDENTS_STAGING.STUDENTS_STG.STUDENTS_MERGED_DB and create the view STUDENTS_SEMANTIC.STUDENTS.VW_STUDENTS using execute() method.

Scope for Enhancements:

1. The snowflake table to have audit columns like record_load_ts, load_user etc
2. The logging and error handling can be enhanced.
3. Better way to handle secrets.
4. Better organize the code.
5. Define the snowflake roles for better governance.


Output
STUDENTS_STAGING.STUDENTS_STG.STUDENTS_MERGED_DB




STUDENTS_SEMANTIC.STUDENTS.VW_STUDENTS





![image](https://github.com/muppireddyharish/Student_Grades/assets/167728482/fde2817a-309c-432a-83ad-ee47662262ae)
