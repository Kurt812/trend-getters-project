from os import environ as ENV
from dotenv import load_dotenv
import streamlit as st
import requests
import psycopg2
from psycopg2.extras import RealDictCursor

load_dotenv()

API_ENDPOINT = ENV["API_ENDPOINT"]


def check_phone_number(phone_number):
    """Check if the phone number exists in the database."""
    try:
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
            "SELECT * FROM \"user\" WHERE phone_number = %s;",
            (phone_number,)
        )
        result = cursor.fetchone()
        conn.close()
        return result is not None
    except Exception as e:
        st.error(f"Database error: {e}")
        return False


def insert_user(first_name, last_name, phone_number):
    """Check if the phone number exists in the database."""
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
        """INSERT INTO "user" (first_name, last_name, phone_number) VALUES (%s, %s, %s)""",
        (first_name, last_name, phone_number,)
    )
    conn.commit()
    conn.close()


st.title("Trend Getter")
st.write("Submit your details to track trends.")

# Step 1: Collect user name and phone number
if "user_verified" not in st.session_state:
    st.session_state["user_verified"] = False

if not st.session_state["user_verified"]:
    with st.form("user_form"):
        user_first = st.text_input("Enter your first name:")
        user_last = st.text_input("Enter your last name:")
        phone_number = st.text_input("Enter your phone number:")
        submit_user_button = st.form_submit_button("Submit")

    if submit_user_button:
        if user_first.strip() and user_last.strip() and phone_number.strip():
            st.session_state["user_verified"] = True
            st.session_state["phone_number"] = phone_number.strip()
            st.session_state["user_first"] = user_first.strip()
            st.session_state["user_last"] = user_last.strip()
            if check_phone_number(phone_number.strip()):
                st.success("Phone number verified!")
                st.rerun()
            else:
                insert_user(
                    st.session_state["user_first"], st.session_state["user_last"], st.session_state["phone_number"])
                st.success("Phone number not found. Registering to database")
                st.rerun()
        else:
            st.warning("Please enter both your name and phone number.")
else:
    st.write(f"Welcome back, {st.session_state['user_first']}!")

    # Step 2: Collect topic and subscription details
    with st.form("topic_form"):
        topic_name = st.text_input("Enter the topic or keyword:")
        notification_threshold = st.number_input(
            "Set notification threshold (optional):", min_value=0, value=0
        )
        subscription_status = st.selectbox(
            "Subscription Status:", ["enabled", "disabled"])
        submit_topic_button = st.form_submit_button("Submit Topic")

    if submit_topic_button:
        if topic_name.strip():
            data = {
                "topic_name": topic_name.strip(),
                "notification_threshold": notification_threshold,
                "user_first": st.session_state["user_first"],
                "user_last": st.session_state["user_last"],
                "phone_number": st.session_state["phone_number"],
                "subscription_status": subscription_status,
            }

            try:
                response = requests.post(API_ENDPOINT, json=data)
                if response.status_code == 200:
                    st.success("Topic submitted successfully!")
                else:
                    st.error(f"Error: {response.json().get(
                        'message', 'Unknown error')}")
            except requests.exceptions.RequestException as e:
                st.error(f"Failed to connect to the API. Error: {e}")
        else:
            st.warning("Please enter a valid topic.")
