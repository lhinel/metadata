import os
import psycopg2
from PyPDF2 import PdfReader
from datetime import datetime, timedelta

# Database connection parameters
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'meta'
DB_USER = 'postgres'
DB_PASSWORD = 'admin'

def create_table_if_not_exists(conn):
    # Create a table if it doesn't exist
    with conn.cursor() as cursor:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pdf_metadata (
                id SERIAL PRIMARY KEY,
                path TEXT,
                title TEXT,
                author TEXT,
                subject TEXT,
                creator TEXT,
                creation_date TIMESTAMP,
                modification_date TIMESTAMP,
                content TEXT
            );
        ''')
        conn.commit()

def extract_text_from_pdf(pdf_path):
    try:
        with open(pdf_path, 'rb') as file:
            pdf_reader = PdfReader(file)
            text = ''
            for page_num in range(len(pdf_reader.pages)):
                text += pdf_reader.pages[page_num].extract_text()
            return text
    except Exception as e:
        print(f"Error extracting text from {pdf_path}: {e}")
        return None

def convert_pdf_date_to_timestamp(pdf_date):
    try:
        # Extract year, month, day, hour, minute, second from the PDF date
        year = int(pdf_date[2:6])
        month = int(pdf_date[6:8])
        day = int(pdf_date[8:10])
        hour = int(pdf_date[10:12])
        minute = int(pdf_date[12:14])
        second = int(pdf_date[14:16])

        # Create a datetime object and adjust the timezone
        dt = datetime(year, month, day, hour, minute, second)
        dt_utc = dt - timedelta(hours=int(pdf_date[16:19]))

        return dt_utc.strftime('%Y-%m-%d %H:%M:%S')

    except ValueError as e:
        print(f"Error converting PDF date to timestamp: {e}")
        return None

def extract_metadata_from_pdf(pdf_path):
    try:
        with open(pdf_path, 'rb') as file:
            pdf_reader = PdfReader(file)
            metadata = pdf_reader.metadata
            return {
                'Title': metadata.title,
                'Author': metadata.author,
                'Subject': metadata.subject,
                'Creator': metadata.creator,
                'Creation Date': convert_pdf_date_to_timestamp(metadata.get('/CreationDate')),
                'Modification Date': convert_pdf_date_to_timestamp(metadata.get('/ModDate')),
            }
    except Exception as e:
        print(f"Error extracting metadata from {pdf_path}: {e}")
        return None

def save_to_database(pdf_path, metadata, content):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        create_table_if_not_exists(conn)

        with conn.cursor() as cursor:
            cursor.execute('''
                INSERT INTO pdf_metadata
                (path, title, author, subject, creator, creation_date, modification_date, content)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
            ''', (pdf_path, metadata['Title'], metadata['Author'], metadata['Subject'],
                  metadata['Creator'], metadata['Creation Date'], metadata['Modification Date'], content))

            id = cursor.fetchone()[0]
            print(f"Data with ID {id} saved to database.")

            conn.commit()

    except Exception as e:
        print(f"Error saving to database: {e}")
    finally:
        conn.close()

def scan_directory(directory_path):
    for root, dirs, files in os.walk(directory_path):
        for file_name in files:
            if file_name.lower().endswith('.pdf'):
                pdf_path = os.path.join(root, file_name)
                text = extract_text_from_pdf(pdf_path)
                metadata = extract_metadata_from_pdf(pdf_path)

                if text and metadata:
                    print(f"Processing {pdf_path}...")
                    save_to_database(pdf_path, metadata, text)

def main():
    designated_directory = input("Enter the path of the designated directory: ")
    if os.path.exists(designated_directory):
        scan_directory(designated_directory)
    else:
        print("Invalid directory path. Please provide a valid path.")

if __name__ == "__main__":
    main()
