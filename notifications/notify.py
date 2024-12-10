from os import environ as ENV
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
            ARRAY_AGG(kr.total_mentions ORDER BY kr.date_and_hour DESC) AS mentions
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
        u.email,
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


def send_email(email, message):
    ses_client = boto3.client("ses", region_name="eu-west-2")
    CHARSET = "UTF-8"

    response = ses_client.send_email(
        Destination={
            "ToAddresses": [
                email,
            ],
        },
        Message={
            "Body": {
                "Html": {
                    "Charset": CHARSET,
                    "Data": message,
                },
                "Text": {
                    "Charset": CHARSET,
                    "Data": "There's been an update in your subscription trends. Check the dashboard for details.",
                }
            },
            "Subject": {
                "Charset": CHARSET,
                "Data": "Trend Getter Update",
            },
        },
        Source="trainee.ridwan.hamid@sigmalabs.co.uk",
    )
    print(response)


def lambda_handler(event, context):
    load_dotenv()
    conn, cursor = setup_connection()

    notifications = fetch_keyword_differences(cursor)

    for notification in notifications:
        message = f"""
        <html>
        <head>
        <body style="background-color: #f4f4f4; padding: 20px;">
            <div style="background-color: white; max-width: 600px; margin: auto; padding: 20px; border-radius: 10px; box-shadow: 0 0 10px rgba(0,0,0,0.1);">
                <div class="header">
                    <h2 style="color: #333;">Hello {notification['first_name']}!</h2>
                </div>
                <div style="background-color: #eef; padding: 15px; border-left: 4px solid #a3333e; margin-bottom: 20px;">
                    <p>
                        You are receiving this email because there has been significant activity in your subscription for 
                        <strong>{notification['keyword']}</strong>. Below are the details of the recent trends.
                    </p>
                </div>
                <table style="width: 100%; border-collapse: collapse; margin: 20px 0;">
                    <thead>
                        <tr style="background-color: #a3333e; color: #fff; text-align: left;">
                            <th style="padding: 10px; border: 1px solid #ddd;">Keyword</th>
                            <th style="padding: 10px; border: 1px solid #ddd;">Trend</th>
                            <th style="padding: 10px; border: 1px solid #ddd;">Change</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td style="padding: 10px; border: 1px solid #ddd;">{notification['keyword']}</td>
                            <td style="padding: 10px; border: 1px solid #ddd;">{notification['direction']}</td>
                            <td style="padding: 10px; border: 1px solid #ddd;">{abs(notification['difference'])} mentions</td>
                        </tr>
                    </tbody>
                </table>
                <div style="text-align: center; margin: 20px;">
                    <a href="https://yourdashboardlink.com" style="
                        background-color: #a3333e; 
                        color: white; 
                        padding: 10px 20px; 
                        text-decoration: none; 
                        border-radius: 5px; 
                        font-weight: bold;">View Your Dashboard</a>
                </div>
                <div style="border-top: 1px solid #ddd; margin-top: 20px; padding-top: 10px; font-size: 12px; color: #777; text-align: center;">
                    <p>You are receiving this email as a subscriber of Trend Getter. If you no longer wish to receive updates, 
                    <a href="https://unsubscribe-link.com" style="color: #a3333e;">unsubscribe here</a>.</p>
                    <p>&copy; 2024 Trend Getter Inc. All rights reserved.</p>
                </div>
            </div>
        </body>
        </html>
        """
        send_email(notification['email'], message)
        # print(message)

    cursor.close()
    conn.close()


if __name__ == "__main__":
    lambda_handler(None, None)
