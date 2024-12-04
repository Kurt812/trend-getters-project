import psycopg2
import psycopg2.extras
from os import environ as ENV
from dotenv import load_dotenv


def import_kiosk_data_into_database(keyword: str):
    """Importing the data into the database"""
    conn = psycopg2.connect(
        user=ENV["DB_USERNAME"],
        password=ENV["DB_PASSWORD"],
        host=ENV["DB_HOST"],
        port=ENV["DB_PORT"],
        database=ENV["DB_NAME"]
    )
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute(f"SET SEARCH_PATH TO {ENV["SCHEMA_NAME"]};")
    cursor.execute(
        f"""INSERT INTO topic (topic_name)
        VALUES ('{keyword}')""")
    conn.commit()


def main(keyword: str):

    load_dotenv()
    import_kiosk_data_into_database(keyword)


if __name__ == "__main__":

    main('book')
