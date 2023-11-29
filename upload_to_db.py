import time
import MySQLdb
import pandas as pd
import os
from cuid import cuid
from dotenv import load_dotenv

load_dotenv()

def upload_jobs_to_db():
    # Your MySQL connection parameters
    database = os.getenv('DB')
    username = os.getenv('DB_USERNAME')
    host = os.getenv('DB_HOST')
    password = os.getenv('DBN_PASSWORD')

    conn = MySQLdb.connect(
        host=host,
        user=username,
        passwd=password,
        db=database,
        autocommit=True,
        ssl_mode="VERIFY_IDENTITY",
        ssl={
            "ca": "cacert-2023-08-22.pem"
        }
    )

    # Path to your CSV file
    csv_file_path = 'job-scrapers/linkedinjobs.csv'

    # Read CSV file into a DataFrame
    csv_file_path = 'job-scrapers/linkedinjobs.csv'

    # Read CSV file into a DataFrame
    df = pd.read_csv(csv_file_path).dropna()

    # Establish connection to MySQL
    cursor = conn.cursor()

    # Prepare data for insertion and logging
    successful_rows = []
    failed_rows = []

    for _, row in df.iterrows():
        job_data = (
            cuid(),
            row['company'],
            row['job-title'],
            row['description'],
            row['logo'],
            row['job-link']
        )

        # Attempt to insert each row separately
        insert_query = """
            INSERT INTO JobPost (id, companyName, jobTitle, jobInfo, companyLogo, jobLink)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                companyName = VALUES(companyName),
                jobTitle = VALUES(jobTitle),
                jobInfo = VALUES(jobInfo),
                companyLogo = VALUES(companyLogo)
        """

        try:
            cursor.execute(insert_query, job_data)
            print("Row inserted successfully.")
            successful_rows.append(job_data)
        except Exception as e:
            print(f"Error during insertion: {str(e)}")
            failed_rows.append(job_data)

    # Close cursor and connection
    cursor.close()
    conn.close()

    # Log successful and failed cases
    execution_time = time.strftime("%Y-%m-%d_%H-%M-%S")
    log_file_name = f"{execution_time}_linkedin-jobs.log"
    with open(log_file_name, 'w') as log_file:
        log_file.write(f"Execution Time: {execution_time}\n")

        log_file.write("Successful Rows:\n")
        for row in successful_rows:
            log_file.write(f"Company: {row[1]}, Job Title: {row[2]}, Link: {row[5]}\n")

        log_file.write("\nFailed Rows:\n")
        for row in failed_rows:
            log_file.write(f"Company: {row[1]}, Job Title: {row[2]}, Link: {row[5]}\n")


def initialize_db_connection(host, username, password, database):
    conn = MySQLdb.connect(
        host=host,
        user=username,
        passwd=password,
        db=database,
        autocommit=True,
        ssl_mode="VERIFY_IDENTITY",
        ssl={
            "ca": "cacert-2023-08-22.pem"
        }
    )
    return conn

def delete_all_from_db(conn):
    cursor = conn.cursor()

    cursor = conn.cursor()

    try:
        # Execute the DELETE query to remove all rows from JobPost table
        cursor.execute("DELETE FROM JobPost")

        # Commit the changes and close cursor
        conn.commit()
        print("All jobs deleted from the database successfully.")
    except Exception as e:
        # If there's an exception, rollback changes and print the error
        conn.rollback()
        print(f"Error: {str(e)}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    conn = initialize_db_connection(host, username, password, database)
    # delete_all_from_db(conn)
    upload_jobs_to_db()