"""Trend Getter Dashboard"""
from os import environ as ENV
import re
from dotenv import load_dotenv
import streamlit as st
import requests
import psycopg2
from psycopg2.extras import RealDictCursor

load_dotenv()

API_ENDPOINT = ENV["API_ENDPOINT"]


def get_connection() -> psycopg2.extensions.connection:
    """Establish and return a database connection"""
    return psycopg2.connect(
        user=ENV["DB_USERNAME"],
        password=ENV["DB_PASSWORD"],
        host=ENV["DB_HOST"],
        port=ENV["DB_PORT"],
        database=ENV["DB_NAME"]
    )


def fetch_user_keywords(user_id):
    query = """SELECT k.keyword 
               FROM subscription s
               JOIN keywords k ON s.keywords_id = k.keywords_id
               WHERE s.user_id = %s;"""

    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SET SEARCH_PATH TO %s;",
                   (ENV["SCHEMA_NAME"],))
    cursor.execute(query, (user_id,))
    results = cursor.fetchall()
    return [result["keyword"] for result in results]


def fetch_keyword_id(keyword) -> list:
    """Fetch available keywords from the database."""
    query = "SELECT * FROM keywords WHERE keyword = %s;"
    result = execute_query(query, (keyword,), fetch_one=True)
    return result


def subscribe_to_keyword(user_id, keywords_id, subscription_status, notification_threshold) -> None:
    """Subscribe a user to a keyword with a given threshold, inserting if not exists and updating if exists."""
    # Step 1: Check if the subscription already exists
    check_query = """
        SELECT 1 
        FROM subscription 
        WHERE user_id = %s AND keywords_id = %s;
    """
    result = execute_query(check_query, (user_id, keywords_id),
                           True)  # Assumes fetch_query returns a result list

    if result:  # Record exists
        # Step 2: Update the existing record
        update_query = """
            UPDATE subscription
            SET subscription_status = %s, notification_threshold = %s
            WHERE user_id = %s AND keywords_id = %s;
        """
        execute_query(update_query, (subscription_status,
                      notification_threshold, user_id, keywords_id))
    else:  # Record does not exist
        # Step 3: Insert a new record
        insert_query = """
            INSERT INTO subscription (user_id, keywords_id, subscription_status, notification_threshold)
            VALUES (%s, %s, %s, %s);
        """
        execute_query(insert_query, (user_id, keywords_id,
                      subscription_status, notification_threshold))


def execute_query(query: str, params: tuple = None, fetch_one: bool = False):
    """Execute a query and return results if applicable"""
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SET SEARCH_PATH TO %s;",
                       (ENV["SCHEMA_NAME"],))
        cursor.execute(query, params)
        if fetch_one:
            return cursor.fetchone()
        conn.commit()
        conn.close()
        return None
    except psycopg2.DatabaseError as e:
        st.error(f"Database error: {e}")
        return None


def is_valid_uk_phone_number(phone_number: str) -> bool:
    """Check if the given phone number is a valid UK mobile number starting with +447"""
    pattern = r'^\+447\d{9}$'
    return bool(re.match(pattern, phone_number))


def check_phone_number(phone_number: str) -> bool:
    """Check if the phone number exists in the database"""
    query = "SELECT * FROM \"user\" WHERE phone_number = %s;"
    result = execute_query(query, (phone_number,), fetch_one=True)
    return result is not None


def check_user(phone_number: str, first_name: str, last_name: str) -> bool:
    """Check if the user exists in the database and verify their name."""
    query = """SELECT user_id FROM "user"
               WHERE phone_number = %s 
               AND first_name = %s 
               AND last_name = %s;"""
    result = execute_query(
        query, (phone_number, first_name, last_name), fetch_one=True)
    return result


def insert_user(first_name: str, last_name: str, phone_number: str) -> None:
    """Insert a new user into the database"""
    query = """INSERT INTO "user" (first_name, last_name, phone_number)
               VALUES (%s, %s, %s);"""
    execute_query(query, (first_name, last_name, phone_number))


def submit_topic(data: dict) -> None:
    """Submit topic details to the API"""
    try:
        response = requests.post(API_ENDPOINT, json=data, timeout=10)
        if response.status_code == 200:
            st.success("✅ Topic submitted successfully!")
        else:
            st.error(f"""Error: {response.json().get(
                'message', 'Unknown error')}""")
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to connect to the API. Error: {e}")


def user_verification() -> None:
    """UI for verifying the user's phone number"""
    with st.form("user_form", clear_on_submit=True):
        st.markdown('<div class="form-header">Enter Your Details</div>',
                    unsafe_allow_html=True)
        user_first = st.text_input("First Name", help="Enter your first name")
        user_last = st.text_input("Last Name", help="Enter your last name")
        phone_number = st.text_input(
            "Phone Number", help="Enter a valid phone number")
        submit_user_button = st.form_submit_button("Submit")

    if submit_user_button:
        if user_first.strip() and user_last.strip() and phone_number.strip():
            phone_number = phone_number.strip()
            user_first = user_first.strip()
            user_last = user_last.strip()

            st.session_state.update({
                "user_verified": True,
                "phone_number": phone_number,
                "user_first": user_first,
                "user_last": user_last,
                "is_new_user": False
            })
            if check_phone_number(phone_number.strip()):
                user_id = check_user(phone_number, user_first, user_last)
                if user_id is not None:
                    st.session_state["user_id"] = user_id
                    st.success("Phone number and name verified!")
                else:
                    st.session_state["verification_error"] = (
                        "The phone number exists, but the name does not match. Please try again."
                    )
                    st.session_state["user_verified"] = False
            else:
                if not is_valid_uk_phone_number(phone_number):
                    st.session_state["user_verified"] = False
                    st.session_state["verification_error"] = (
                        "Please enter a valid UK phone number (e.g., starting with +447 and 9 digits after that)."
                    )
                else:
                    insert_user(user_first, user_last, phone_number)
                    user_id = check_user(phone_number, user_first, user_last)
                    st.session_state["user_id"] = user_id
                    print("USERID", user_id)
                    st.session_state["is_new_user"] = True
                    st.success(
                        "Phone number not found. Registering to database.")
            st.rerun()
        else:
            st.warning("Please enter both your name and phone number.")

    if "verification_error" in st.session_state and st.session_state["verification_error"]:
        st.error(st.session_state["verification_error"])


def topic_and_subscription_ui() -> None:
    """UI for topic entry and keyword subscription."""
    if not st.session_state.get("is_new_user", False):
        st.write(f"Welcome back, {st.session_state['user_first']}!")
    else:
        st.write(f"Greetings, {st.session_state['user_first']}!")

    with st.sidebar:
        st.header("Topic Management")
        st.subheader("Enter New Topic")
        with st.form("topic_form"):
            new_topic = st.text_input("Enter the topic or keyword:")
            print('q', new_topic, 'q')
            submit_topic_button = st.form_submit_button("Add Topic")

        if submit_topic_button:
            if new_topic.strip():
                topic_data = {
                    "topic_name": new_topic.strip()
                }
                submit_topic(topic_data)
            else:
                st.warning("Please enter a valid topic.")

        # Keyword Subscription
        st.subheader("Subscribe to Keywords")
        existing_keywords = fetch_user_keywords(
            st.session_state["user_id"]["user_id"])
        if new_topic.strip() != '' and new_topic.strip() not in existing_keywords:
            existing_keywords.append(new_topic.strip())

        if existing_keywords:
            with st.form("subscription_form"):
                selected_keyword = st.selectbox(
                    "Choose a keyword to subscribe:", existing_keywords)
                subscription_status = st.selectbox(
                    "Subscription Status:", ["disabled", "enabled"]
                ) == "enabled"
                subscription_threshold = st.number_input(
                    "Set notification threshold:", min_value=0, value=0
                )
                subscribe_button = st.form_submit_button("Subscribe")

            if subscribe_button:
                keyword_id = fetch_keyword_id(selected_keyword)["keywords_id"]
                print(st.session_state["user_id"]["user_id"])
                print(keyword_id)
                print(subscription_threshold)
                if not subscription_status:
                    subscription_threshold = None
                if selected_keyword.strip():
                    subscribe_to_keyword(
                        st.session_state["user_id"]["user_id"], keyword_id, subscription_status, subscription_threshold
                    )
                    st.success(f"Subscribed to '{selected_keyword}' with a threshold of {
                               subscription_threshold}!")
                else:
                    st.warning("Please select a valid keyword.")
        else:
            st.info("No keywords available. Add a topic to create keywords.")


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
        Enter keywords to see visualizations
        </div>
        """,
        unsafe_allow_html=True
    )


def main() -> None:
    """Main function to render the Streamlit app."""
    st.title("Trend Getter Dashboard")

    if "user_verified" not in st.session_state:
        st.session_state["user_verified"] = False

    if not st.session_state["user_verified"]:
        st.write("Submit your details to track trends.")
        user_verification()
    else:
        topic_and_subscription_ui()
        if st.session_state.get("is_new_user", False):
            display_center_message()


if __name__ == "__main__":
    main()
