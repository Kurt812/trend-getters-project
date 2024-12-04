from os import environ as ENV
from dotenv import load_dotenv
import streamlit as st
import requests
import psycopg2
from psycopg2.extras import RealDictCursor

load_dotenv()

API_ENDPOINT = ENV["API_ENDPOINT"]


def get_connection():
    """Establish and return a database connection"""
    return psycopg2.connect(
        user=ENV["DB_USERNAME"],
        password=ENV["DB_PASSWORD"],
        host=ENV["DB_HOST"],
        port=ENV["DB_PORT"],
        database=ENV["DB_NAME"]
    )


def execute_query(query, params=None, fetch_one=False):
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
    except psycopg2.DatabaseError as e:
        st.error(f"Database error: {e}")
        return None


def check_phone_number(phone_number):
    """Check if the phone number exists in the database"""
    query = "SELECT * FROM \"user\" WHERE phone_number = %s;"
    result = execute_query(query, (phone_number,), fetch_one=True)
    return result is not None


def check_user(phone_number, first_name, last_name):
    """Check if the user exists in the database and verify their name."""
    query = """SELECT * FROM "user" 
               WHERE phone_number = %s 
               AND first_name = %s 
               AND last_name = %s;"""
    result = execute_query(
        query, (phone_number, first_name, last_name), fetch_one=True)
    return result is not None


def insert_user(first_name, last_name, phone_number):
    """Insert a new user into the database"""
    query = """INSERT INTO "user" (first_name, last_name, phone_number) 
               VALUES (%s, %s, %s);"""
    execute_query(query, (first_name, last_name, phone_number))


def submit_topic(data):
    """Submit topic details to the API"""
    try:
        response = requests.post(API_ENDPOINT, json=data, timeout=10)
        if response.status_code == 200:
            st.success("✅ Topic submitted successfully!")
        else:
            st.error(f"Error: {response.json().get(
                'message', 'Unknown error')}")
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to connect to the API. Error: {e}")


def user_verification():
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
                if check_user(phone_number, user_first, user_last):
                    st.success("Phone number and name verified!")
                else:
                    st.session_state["verification_error"] = (
                        "The phone number exists, but the name does not match. Please try again."
                    )
                    st.session_state["user_verified"] = False
            else:
                insert_user(user_first, user_last, phone_number)
                st.session_state["is_new_user"] = True
                st.success("Phone number not found. Registering to database.")
            st.rerun()
        else:
            st.warning("Please enter both your name and phone number.")

    if "verification_error" in st.session_state and st.session_state["verification_error"]:
        st.error(st.session_state["verification_error"])


def topic_submission():
    """UI for submitting a topic in the left panel"""
    if not st.session_state.get("is_new_user", False):
        st.write(f"Welcome back, {st.session_state['user_first']}!")
    else:
        st.write(f"Greetings, {st.session_state['user_first']}!")

    with st.sidebar:
        st.header("Topic Submission")
        with st.form("topic_form"):
            topic_name = st.text_input("Enter the topic or keyword:")
            subscription_status = st.selectbox(
                "Subscription Status:", ["enabled", "disabled"]
            )
            notification_threshold = st.number_input(
                "Set notification threshold (optional):", min_value=0, value=0
            )
            submit_topic_button = st.form_submit_button("Submit Topic")

        if submit_topic_button:
            if topic_name.strip():
                topic_data = {
                    "topic_name": topic_name.strip(),
                    "notification_threshold": notification_threshold,
                    "user_first": st.session_state["user_first"],
                    "user_last": st.session_state["user_last"],
                    "phone_number": st.session_state["phone_number"],
                    "subscription_status": subscription_status,
                }
                submit_topic(topic_data)
            else:
                st.warning("Please enter a valid topic.")


def display_center_message():
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


def main():
    """Main function to render the Streamlit app."""
    st.title("Trend Getter")

    if "user_verified" not in st.session_state:
        st.session_state["user_verified"] = False

    if not st.session_state["user_verified"]:
        st.write("Submit your details to track trends.")
        user_verification()
    else:
        topic_submission()
        if st.session_state.get("is_new_user", False):
            display_center_message()


if __name__ == "__main__":
    main()
