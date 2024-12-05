# """Script to load data into RDS"""
# from os import environ as ENV
# import psycopg2
# import psycopg2.extras
# from dotenv import load_dotenv


# def setup_connection():
#     """Retrieve database connection and cursor"""
#     conn = psycopg2.connect(
#         user=ENV["DB_USERNAME"],
#         password=ENV["DB_PASSWORD"],
#         host=ENV["DB_HOST"],
#         port=ENV["DB_PORT"],
#         database=ENV["DB_NAME"]
#     )
#     cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
#     cursor.execute(f"SET SEARCH_PATH TO {ENV['SCHEMA_NAME']};")
#     return conn, cursor


# def insert_keywords(conn, cursor, topics: list[str]) -> list[int]:
#     """Insert keywords into keywords table from topic"""
#     keyword_ids = []
#     for keyword in topics:
#         cursor.execute(
#             "SELECT keywords_id FROM keywords WHERE keyword = %s", (keyword,))
#         existing_id = cursor.fetchone()

#         if existing_id is None:
#             cursor.execute("""INSERT INTO keywords (keyword)
#                             VALUES (%s) RETURNING keywords_id""", (keyword,))
#             conn.commit()
#             new_keyword_id = cursor.fetchone()['keywords_id']
#             keyword_ids.append(new_keyword_id)
#             continue

#         keyword_ids.append(existing_id['keywords_id'])

#     return keyword_ids


# def find_unique_keywords(cursor):
#     cursor.execute(
#         """SELECT DISTINCT keywords_id
#          FROM subscription""")
#     keywords = cursor.fetchall()
#     return keywords


# def last_recordings(cursor, keywords_id):
#     cursor.execute(
#         """SELECT *
#          FROM keyword_recordings
#          WHERE keywords_id = %s
#          ORDER BY hour_of_day DESC
#          LIMIT 2;""", (keywords_id,))
#     recordings = cursor.fetchall()
#     return recordings


# def get_subscriptions(cursor):
#     cursor.execute(
#         """SELECT *
#            FROM subscription
#            WHERE subscription_status = TRUE"""
#     )
#     subscriptions = cursor.fetchall()
#     return subscriptions


# def get_keyword(cursor, keywords_id):
#     cursor.execute(
#         """SELECT keyword
#            FROM keywords
#            WHERE keywords_id = %s""", (keywords_id,)
#     )
#     keyword = cursor.fetchone()["keyword"]
#     return keyword


# def main():
#     load_dotenv()
#     differences = {}
#     conn, cursor = setup_connection()
#     unique_keywords = find_unique_keywords(cursor)
#     print(unique_keywords)
#     print()
#     for i in unique_keywords:
#         numbers = [j["total_mentions"]
#                    for j in last_recordings(cursor, i["keywords_id"])]
#         print(i["keywords_id"], last_recordings(cursor, i["keywords_id"]))
#         if len(numbers) > 1:
#             differences[i["keywords_id"]] = abs(numbers[0]-numbers[1])
#         else:
#             differences[i["keywords_id"]] = None
#     print(differences)
#     print()
#     subscriptions = get_subscriptions(cursor)

#     for i in subscriptions:
#         print("Subscription:", i)
#         if differences[i["keywords_id"]] is not None:
#             if int(i["notification_threshold"]) <= differences[i["keywords_id"]]:
#                 print(f"Sending notification to {
#                       i["user_id"]} for the following keyword")
#                 print(get_keyword(cursor, i["keywords_id"]))


# if __name__ == "__main__":
#     main()


from os import environ as ENV
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv


def setup_connection():
    """Retrieve database connection and cursor"""
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


def fetch_keyword_differences(cursor):
    """Fetch differences in keyword mentions and match them
    with subscription thresholds and user information"""
    query = """
    WITH recent_mentions AS (
        SELECT 
            kr.keywords_id,
            ARRAY_AGG(kr.total_mentions ORDER BY kr.hour_of_day DESC) AS mentions
        FROM keyword_recordings kr
        GROUP BY kr.keywords_id
    ),
    keyword_differences AS (
        SELECT 
            rm.keywords_id,
            CASE 
                WHEN CARDINALITY(rm.mentions) > 1 THEN ABS(rm.mentions[1] - rm.mentions[2])
                ELSE NULL
            END AS difference
        FROM recent_mentions rm
    )
    SELECT 
        s.user_id,
        u.first_name,
        u.last_name,
        k.keyword,
        s.notification_threshold,
        kd.difference
    FROM subscription s
    JOIN "user" u ON s.user_id = u.user_id
    JOIN keywords k ON s.keywords_id = k.keywords_id
    JOIN keyword_differences kd ON s.keywords_id = kd.keywords_id
    WHERE s.subscription_status = TRUE
      AND kd.difference IS NOT NULL
      AND kd.difference >= s.notification_threshold;
    """
    cursor.execute(query)
    return cursor.fetchall()


def main():
    load_dotenv()
    conn, cursor = setup_connection()

    notifications = fetch_keyword_differences(cursor)

    for notification in notifications:
        print(f"Sending notification to {notification['first_name']} {notification['last_name']} "
              f"for keyword '{notification['keyword']}' with a difference of {notification['difference']}.")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
