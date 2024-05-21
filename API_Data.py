import csv
import requests
import time
import psycopg2
import logging
from crossref_commons.retrieval import get_publication_as_json
from retrying import retry
import json


logging.basicConfig(filename='API.log', filemode='a', format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.INFO)


@retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def get_citation_count(doi):
    try:
        response = requests.get(f'https://opencitations.net/index/api/v1/citation-count/{doi}')
        if response.status_code == 200:
            json_response = response.json()
            if json_response:
                citation_count = json_response[0]['count']
                return citation_count
        return None
    except Exception as e:
        logging.error("An error occurred while fetching citation count: %s", str(e))
        return None

@retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def get_citation_dois(doi):
    url = f'https://opencitations.net/index/api/v1/citations/{doi}'
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return [mod['cited'] for mod in data] if data else []
        else:
            logging.info("Failed to fetch citation DOIs. Status code: %s", response.status_code)
            return []
    except Exception as e:
        logging.error("An error occurred while fetching citation DOIs: %s", str(e))
        return []

def establish_database_connection():
    try:
        conn = psycopg2.connect(database="xxxx",
                                host="xxxxx",
                                user="xxxx",
                                password="xxxx",
                                port="5432")
        return conn
    except Exception as e:
        logging.error("Error connecting to the database: %s", str(e))
        raise

def create_table_if_not_exists(conn):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS mod.crossref_api_data (
        ID SERIAL PRIMARY KEY,
        DOI TEXT,
        confy_id TEXT,
        DOI_Type TEXT,
        Journal_Title TEXT,
        Article_Title TEXT,
        Volume TEXT,
        First_Page TEXT,
        Year TEXT,
        Authors TEXT,
        Publisher TEXT,
        Publication_Date TEXT,
        Citation_DOI_Num JSONB,
        Citation_Count TEXT
    );
    """
    with conn.cursor() as cursor:
        cursor.execute(create_table_query)
        conn.commit()

def insert_data_into_table(conn, data):
    insert_query = """
    INSERT INTO mod.crossref_api_data (DOI, confy_id, DOI_Type, Journal_Title, Article_Title, Volume, First_Page, Year, Authors, Publisher, Publication_Date, Citation_DOI_Num, Citation_Count)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    with conn.cursor() as cursor:
        cursor.execute(insert_query, data)
        conn.commit()
    print(f"Inserted row: {data}")

def process_row(row, conn):
    doi = str(row[1])
    try:
        doi_details = get_publication_as_json(doi)
    except Exception as e:
        logging.error("An error occurred while fetching publication details for DOI %s: %s", doi, str(e))
        doi_details = {}

    if isinstance(doi_details, dict):
        author_names = [f"{author.get('given', '')} {author.get('family', '')}" for author in doi_details.get('author', [])]
        author_list = ", ".join(author_names)
        citation_dois = get_citation_dois(doi)
        data = (
            row[1],
            row[2], 
            doi_details.get('type', '') or None,
            ", ".join(doi_details.get('container-title', [])) if isinstance(doi_details.get('container-title'), list) else None,
            ", ".join(doi_details.get('title', [])) if isinstance(doi_details.get('title'), list) else None,
            doi_details.get('volume', None),
            doi_details.get('page', None),
            doi_details.get('published-online', {}).get('date-parts', [[None]])[0][0] if doi_details.get('published-online') else None,
            author_list,
            doi_details.get('publisher', None),
            "-".join(map(str, doi_details.get('published-online', {}).get('date-parts', [[None]])[0])) if doi_details.get('published-online') else None,
            json.dumps(citation_dois) if citation_dois else None,
            get_citation_count(doi)
        )
    else:
        data = (
            row[1],
            row[2],
            None, None, None, None, None, None, None, None, None, None, None
        )

    insert_data_into_table(conn, data)

def process_rows(rows, conn):
    for row in rows:
        process_row(row, conn)
        time.sleep(1)

def main():
    conn = establish_database_connection()
    create_table_if_not_exists(conn)

    sql = """SELECT id, doi, confy_id FROM eudl.content WHERE doi_resolves = true"""
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()

    if rows:
        logging.info("Data fetched successfully.")
    else:
        logging.error("Failed to fetch data.")

    process_rows(rows, conn)

    logging.info("Data saved to the database.")
    conn.close()

if __name__ == "__main__":
    main()


