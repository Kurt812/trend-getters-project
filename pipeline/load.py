from os import environ as ENV
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from extract import main as downloader
from transform import main as optimus_prime


def setup_connection():
    conn = psycopg2.connect(
        user=ENV["DB_USERNAME"],
        password=ENV["DB_PASSWORD"],
        host=ENV["DB_HOST"],
        port=ENV["DB_PORT"],
        database=ENV["DB_NAME"]
    )
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute(f"SET SEARCH_PATH TO {ENV['SCHEMA_NAME']};")
    return conn, cursor


def insert_topics(conn, cursor, topics: list[str]) -> list[int]:
    """Insert user given topics into the topic table"""
    topic_name = ",".join(sorted(topics))
    cursor.execute(
        "SELECT topic_id FROM topic WHERE topic_name = %s", (topic_name,))
    existing_id = cursor.fetchone()

    if existing_id is None:
        cursor.execute("""INSERT INTO topic (topic_name)
                        VALUES (%s) RETURNING topic_id""", (topic_name,))
        conn.commit()
        new_topic_id = cursor.fetchone()['topic_id']
        return new_topic_id, True

    return existing_id['topic_id'], False


def insert_keywords(conn, cursor, topics: list[str]) -> list[int]:
    """Insert keywords into keywordw tabl from topice"""
    keyword_ids = []
    for keyword in topics:
        cursor.execute(
            "SELECT keywords_id FROM keywords WHERE keyword = %s", (keyword,))
        existing_id = cursor.fetchone()

        if existing_id is None:
            cursor.execute("""INSERT INTO keywords (keyword)
                            VALUES (%s) RETURNING keywords_id""", (keyword,))
            conn.commit()
            new_keyword_id = cursor.fetchone()['keywords_id']
            keyword_ids.append(new_keyword_id)
            continue

        keyword_ids.append(existing_id['keywords_id'])

    return keyword_ids


def assign_keywords(conn, cursor, topic_id: int, keyword_ids: int, is_new_topic: bool) -> None:
    """Assign keywords"""
    if is_new_topic:
        for id in keyword_ids:
            cursor.execute("""INSERT INTO keyword_assignment (topic_id, keywords_id)
                                VALUES (%s, %s)""", (topic_id, id))
        conn.commit()


def insert_trend_data():
    """"""
    ...


def main(topics: list[str]):
    conn, cursor = setup_connection()
    load_dotenv()
    topic_id, is_new_topic = insert_topics(conn, cursor, topics)
    print(topic_id)
    keyword_ids = insert_keywords(conn, cursor, topics)
    print(keyword_ids)
    assign_keywords(conn, cursor, topic_id, keyword_ids, is_new_topic)


if __name__ == "__main__":
    topics = ['strawberries', 'chocolate']  # this
    main(topics)
    extracted_dataframe = downloader(topics)
    final_datafram = optimus_prime(extracted_dataframe)
    final_datafram.to_csv("output.csv", index=False)
    # unique_names = final_datafram["keyword_id"].unique()
    # print(unique_names)
    # main('book')
