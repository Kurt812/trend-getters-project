from dotenv import load_dotenv
from os import environ as ENV
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import cursor
import streamlit as st


def get_connection() -> tuple:
    """Establish and return a database connection"""
    conn = psycopg2.connect(
        user=ENV["DB_USERNAME"],
        password=ENV["DB_PASSWORD"],
        host=ENV["DB_HOST"],
        port=ENV["DB_PORT"],
        database=ENV["DB_NAME"]
    )
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SET SEARCH_PATH TO %s;",
                   (ENV["SCHEMA_NAME"],))
    return conn, cursor


def subscribe_to_keyword(user_id: int, keywords_id: int, subscription_status: bool,
                         notification_threshold: int) -> None:
    """Subscribe a user to a keyword with a given threshold,
    inserting if not exists and updating if exists"""
    check_query = """
        SELECT 1
        FROM subscription
        WHERE user_id = %s AND keywords_id = %s;
    """
    result = execute_query(check_query, (user_id, keywords_id), True)

    if result:
        update_query = """
            UPDATE subscription
            SET subscription_status = %s, notification_threshold = %s
            WHERE user_id = %s AND keywords_id = %s;
        """
        execute_query(update_query, (subscription_status,
                      notification_threshold, user_id, keywords_id))
    else:
        insert_query = """
            INSERT INTO subscription (user_id, keywords_id, subscription_status, notification_threshold)
            VALUES (%s, %s, %s, %s);
        """
        execute_query(insert_query, (user_id, keywords_id,
                      subscription_status, notification_threshold))


def execute_query(query: str, params: tuple = None, fetch_one: bool = False) -> dict:
    """Execute a query and return results if applicable"""
    try:
        conn, cursor = get_connection()
        cursor.execute(query, params)
        if fetch_one:
            return cursor.fetchone()
        conn.commit()
        conn.close()
        return None
    except psycopg2.DatabaseError as e:
        st.error(f"Database error: {e}")
        return None


def fetch_keyword_id(keyword: str) -> list:
    """Fetch available keywords from the database"""
    query = "SELECT keywords_id FROM keywords WHERE keyword = %s;"
    result = execute_query(query, (keyword,), fetch_one=True)
    return result


def process_subscription(selected_keyword: str, subscription_status: bool,
                         subscription_threshold: int) -> None:
    """Processes the subscription to a selected keyword"""
    keyword_id = fetch_keyword_id(selected_keyword)["keywords_id"]
    if not subscription_status:
        subscription_threshold = None
    if selected_keyword.strip():
        if selected_keyword and subscription_status:
            subscribe_to_keyword(
                st.session_state["user_id"]["user_id"],
                keyword_id,
                subscription_status,
                subscription_threshold
            )
            st.success(
                f"""You will be notified if the mentions count over the last hour
                for '{selected_keyword}' has risen or fallen by {subscription_threshold}."""
            )
        else:
            st.success(f"""You have added {selected_keyword} to your list of topics.
                       You will *not* receive notifications""")
    else:
        st.warning("Please select a valid keyword.")


def subscription_form(existing_keywords: list) -> None:
    """Displays the form for subscribing to keywords"""
    st.subheader("Subscribe to Keywords")

    if existing_keywords:
        with st.form("subscription_form"):
            selected_keyword = st.selectbox(
                "Choose a keyword to subscribe:", existing_keywords
            )
            subscription_status = st.selectbox(
                "Subscription Status:", ["disabled", "enabled"]
            ) == "enabled"
            subscription_threshold = st.number_input(
                "Set notification threshold:", min_value=0, value=0
            )
            subscribe_button = st.form_submit_button("Subscribe")
            if subscribe_button:
                process_subscription(
                    selected_keyword, subscription_status, subscription_threshold
                )
    else:
        st.info("No keywords available. Add a topic to create keywords.")


def fetch_user_keywords(user_id: int) -> list[str]:
    """Fetch a user's existing keywords"""
    conn, cursor = get_connection()
    try:
        query = """SELECT k.keyword
                   FROM subscription s
                   JOIN keywords k ON s.keywords_id = k.keywords_id
                   WHERE s.user_id = %s;"""
        cursor.execute(query, (user_id,))
        results = cursor.fetchall()
        return [result["keyword"] for result in results]
    finally:
        conn.close()


def display_keywords(existing_keywords: list, new_topic: str) -> list:
    """Displays existing keywords and appends the new topic if valid"""
    if new_topic and new_topic not in existing_keywords:
        existing_keywords.append(new_topic)
    return existing_keywords


def display_center_message() -> None:
    """Display a grey message in the center of the screen for new users"""
    st.markdown(
        """
        <div style="
            display: flex;
            justify-content: center;
            align-items: center;
            height: 80vh;
            color: grey;
            font-size: 20px;
        ">
        Please log in on the home page to subscribe to topics.
        </div>
        """,
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    if "user_id" in st.session_state:
        existing_keywords = fetch_user_keywords(
            st.session_state["user_id"]["user_id"]
        )
        existing_keywords = display_keywords(
            existing_keywords, st.session_state["new_topic"])
        subscription_form(existing_keywords)
    else:
        display_center_message()
