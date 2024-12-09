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
EMAIL_REGEX = """(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\\.[a-z0-9!#$%&'*+/
=?^_`{|}~-]+)*|\"(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21\\x23-\\x5b\\
x5d-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])*\")@(?:(?:[a-z0-9](?:
[a-z0-9-]*[a-z0-9])?\\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\\[(?:(?:25[0-5]|
2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|
a-z0-9-]*[a-z0-9]:(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21-\\x5a\\x53-\\x7f]|
\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])+)\\])"""


def get_connection() -> psycopg2.extensions.connection:
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


def fetch_user_keywords(user_id):
    """Fetch a user's existing keywords"""
    query = """SELECT k.keyword
               FROM subscription s
               JOIN keywords k ON s.keywords_id = k.keywords_id
               WHERE s.user_id = %s;"""

    _, cursor = get_connection()
    cursor.execute(query, (user_id,))
    results = cursor.fetchall()
    return [result["keyword"] for result in results]


def fetch_keyword_id(keyword) -> list:
    """Fetch available keywords from the database"""
    query = "SELECT * FROM keywords WHERE keyword = %s;"
    result = execute_query(query, (keyword,), fetch_one=True)
    return result


def subscribe_to_keyword(user_id, keywords_id, subscription_status, notification_threshold) -> None:
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


def execute_query(query: str, params: tuple = None, fetch_one: bool = False):
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


def is_valid_email(email: str) -> bool:
    """Check if entered email is valid"""
    return bool(re.match(EMAIL_REGEX, email))


def check_email_exists(email: str) -> bool:
    """Check if the email exists in the database"""
    query = "SELECT * FROM \"user\" WHERE email = %s;"
    result = execute_query(query, (email,), fetch_one=True)
    return result is not None


def check_user(email: str, first_name: str, last_name: str) -> bool:
    """Check if the user exists in the database and verify their name"""
    query = """SELECT user_id FROM "user"
               WHERE email = %s
               AND first_name = %s
               AND last_name = %s;"""
    result = execute_query(
        query, (email, first_name, last_name), fetch_one=True)
    return result


def insert_user(first_name: str, last_name: str, email: str) -> None:
    """Insert a new user into the database"""
    query = """INSERT INTO "user" (first_name, last_name, email)
               VALUES (%s, %s, %s);"""
    execute_query(query, (first_name, last_name, email))


def submit_topic(data: dict) -> None:
    """Submit topic details to the API"""
    try:
        response = requests.post(API_ENDPOINT, json=data, timeout=1000)
        if response.status_code == 200:
            st.success("✅ Topic submitted successfully!")
        else:
            st.error(f"""Error: {response.json().get(
                'message', 'Unknown error')}""")
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to connect to the API. Error: {e}")


def display_user_form() -> tuple:
    """Displays the user input form and handles submission"""
    with st.form("user_form", clear_on_submit=True):
        st.markdown(
            '<div class="form-header">Enter Your Details</div>', unsafe_allow_html=True)
        user_first = st.text_input("First Name", help="Enter your first name")
        user_last = st.text_input("Last Name", help="Enter your last name")
        email = st.text_input(
            "Email", help="Enter a valid email")
        submit_user_button = st.form_submit_button("Submit")
    return user_first.strip(), user_last.strip(), email.strip(), submit_user_button


def process_user(user_first: str, user_last: str, email: str) -> None:
    """Processes the user's verification or registration"""
    if not user_first or not user_last or not email:
        st.session_state["verification_warning"] = "Please enter both your name and email."
        st.session_state["user_verified"] = False
        return

    st.session_state.update({
        "user_verified": True,
        "email": email,
        "user_first": user_first,
        "user_last": user_last,
        "is_new_user": False,
        "verification_warning": None
    })

    if check_email_exists(email):
        user_id = check_user(email, user_first, user_last)
        if user_id:
            st.session_state["user_id"] = user_id
            st.success("Email and name verified!")
        else:
            st.session_state.update({
                "user_verified": False,
                "verification_error": """The email exists, but the name does not match. 
                Please try again."""
            })
    else:
        handle_new_user_registration(user_first, user_last, email)


def handle_new_user_registration(user_first: str, user_last: str, email: str) -> None:
    """Registers a new user if the email does not exist"""
    if not is_valid_email(email):
        st.session_state.update({
            "user_verified": False,
            "verification_error": "Please enter a valid email."
        })
        return

    insert_user(user_first, user_last, email)
    user_id = check_user(email, user_first, user_last)
    st.session_state.update({
        "user_id": user_id,
        "is_new_user": True
    })
    st.success("Email not found. Registering to database.")


def display_errors() -> None:
    """Displays verification errors and warnings stored in session state"""
    if st.session_state.get("verification_warning"):
        st.warning(st.session_state["verification_warning"])
    if st.session_state.get("verification_error"):
        st.error(st.session_state["verification_error"])


def user_verification() -> None:
    """Main function for verifying the user's email"""
    user_first, user_last, email, submit_user_button = display_user_form()

    if submit_user_button:
        process_user(user_first, user_last, email)
        st.rerun()

    display_errors()


def display_welcome_message() -> None:
    """Displays a welcome message based on user session state"""
    if not st.session_state.get("is_new_user", False):
        st.write(f"Welcome back, {st.session_state['user_first']}!")
    else:
        st.write(f"Greetings, {st.session_state['user_first']}!")


def topic_entry_form() -> str:
    """Displays the form for entering a new topic"""
    st.subheader("Enter New Topic")
    with st.form("topic_form"):
        new_topic = st.text_input("Enter the topic or keyword:")
        submit_topic_button = st.form_submit_button("Add Topic")
        if submit_topic_button:
            if new_topic.strip():
                topic_data = {"topic_name": new_topic.strip()}
                submit_topic(topic_data)
            else:
                st.warning("Please enter a valid topic.")
    return new_topic.strip()


def display_keywords(existing_keywords: list, new_topic: str) -> list:
    """Displays existing keywords and appends the new topic if valid"""
    if new_topic and new_topic not in existing_keywords:
        existing_keywords.append(new_topic)
    return existing_keywords


def subscription_form(existing_keywords: list) -> None:
    """Displays the form for subscribing to keywords"""
    if existing_keywords:
        st.subheader("Subscribe to Keywords")
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


def topic_and_subscription_ui() -> None:
    """Main UI function for topic entry and keyword subscription"""
    display_welcome_message()

    with st.sidebar:
        st.header("Topic Management")
        new_topic = topic_entry_form()
        existing_keywords = fetch_user_keywords(
            st.session_state["user_id"]["user_id"]
        )
        existing_keywords = display_keywords(existing_keywords, new_topic)
        subscription_form(existing_keywords)


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
    """Main function to render the Streamlit app"""
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
