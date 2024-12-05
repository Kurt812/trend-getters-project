from os import environ as ENV
import os
import boto3
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
                WHEN CARDINALITY(rm.mentions) > 1 THEN rm.mentions[1] - rm.mentions[2]
                ELSE NULL
            END AS difference
        FROM recent_mentions rm
    )
    SELECT 
        s.user_id,
        u.first_name,
        u.last_name,
        u.phone_number,
        k.keyword,
        s.notification_threshold,
        kd.difference,
        CASE 
            WHEN kd.difference > 0 THEN 'increased'
            WHEN kd.difference < 0 THEN 'decreased'
            ELSE 'no change'
        END AS direction
    FROM subscription s
    JOIN "user" u ON s.user_id = u.user_id
    JOIN keywords k ON s.keywords_id = k.keywords_id
    JOIN keyword_differences kd ON s.keywords_id = kd.keywords_id
    WHERE s.subscription_status = TRUE
      AND kd.difference IS NOT NULL
      AND ABS(kd.difference) >= s.notification_threshold;
    """
    cursor.execute(query)
    return cursor.fetchall()


def send_sns_notification(phone_number, message):
    """Send notification via Amazon SNS"""
    sns_client = boto3.client("sns", aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
                              aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"), region_name="eu-west-2")
    try:
        sns_client.publish(
            PhoneNumber=phone_number,
            Message=message,
        )
        print(f"Notification sent to {phone_number}")
    except Exception as e:
        print(f"Failed to send notification to {phone_number}: {e}")


def main():
    load_dotenv()
    conn, cursor = setup_connection()

    notifications = fetch_keyword_differences(cursor)

    for notification in notifications:
        message = (
            f"""Hi {notification['first_name']}, there's been a spike in your subscription for {
                notification['keyword']}. It has {notification['direction']} by {
                abs(notification['difference'])} mentions in the last hour. Check the dashboard for more details."""
        )
        send_sns_notification(notification['phone_number'], message)
        print(message)

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
