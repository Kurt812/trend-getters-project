"""Trend Getter Dashboard"""

from os import environ as ENV
import pandas as pd
import altair as alt
from dotenv import load_dotenv
import streamlit as st
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from email_validator import validate_email, EmailNotValidError
from streamlit_agraph import agraph, Node, Edge, Config


from queries import (get_related_words, get_most_negative_word,
                     get_most_positive_word, get_most_mentioned_word)
from combined_data import main_combine
from predict_mentions import main_predict


load_dotenv()
pd.set_option('display.precision', 2)
API_ENDPOINT = ENV["API_ENDPOINT"]
COLOUR_PALETTE = ['#C4D6B0', '#477998', '#F64740', '#A3333D']
COLOUR_IMAGES = ["https://www.colorhexa.com/c4d6b0.png",
                 "https://www.colorhexa.com/477998.png"]


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


def fetch_keyword_id(keyword: str) -> list:
    """Fetch available keywords from the database"""
    query = "SELECT keywords_id FROM keywords WHERE keyword = %s;"
    result = execute_query(query, (keyword,), fetch_one=True)
    return result


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


def is_valid_email(email: str) -> bool:
    """Check if entered email is valid"""
    try:
        validate_email(email)
        return True
    except EmailNotValidError:
        return False


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
        Enter keywords to see visualizations
        </div>
        """,
        unsafe_allow_html=True
    )


def main() -> None:
    """Main function to render the Streamlit app"""

    st.set_page_config(page_title="Trend Getter",
                       page_icon=":chart_with_upwards_trend:", layout="wide")

    st.markdown(
        """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=League+Spartan:wght@100..900&display=swap');

    html, body, div, span, appview-container, header, footer, [class*="css"] {
        font-family: 'League Spartan', sans-serif;
    }
    </style>
    """,
        unsafe_allow_html=True
    )
    left_title, logo = st.columns([1, 1])
    with left_title:
        st.markdown(
            """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=League+Spartan:wght@100..900&display=swap');

        /* Apply the font globally */
        html, body, [class*="css"] {
            font-family: 'League Spartan', sans-serif;
        }

        .title {
            text-align: left;
            font-size: 50px;
            margin-top: 50px;
            margin-bottom: 20px
            font-weight: bolder;
        }

        </style>
        """,
            unsafe_allow_html=True
        )

        st.markdown("""
             <div></div>       
            <div class="title">Trend Getter</div>
        
        """, unsafe_allow_html=True)
    with logo:
        st.markdown(
            """
        <style>
        .top-right-image {
            position: absolute;
            top: 0;
            right: 0;
            width: 300px;
            margin-bottom: 500px;
        }
        </style>
        <img src="https://raw.githubusercontent.com/Kurt812/trend-getters-project/development/images/logo-no-writing.png" class="top-right-image" alt="Logo">

        """,
            unsafe_allow_html=True)

    _, cursor = get_connection()
    if "user_verified" not in st.session_state:
        st.session_state["user_verified"] = False

    if not st.session_state["user_verified"]:
        st.write("Submit your details to track trends.")
        user_verification()

    else:
        existing_keywords = topic_and_subscription_ui()
        if st.session_state.get("is_new_user", False) and not existing_keywords:
            st.write('Over the last 24 hours:')
            display_new_user_stats(cursor)

        else:
            selected_keywords = get_keyword_filter(existing_keywords)
            combined_data = main_combine()
            filtered_data_list = filter_by_keyword(
                selected_keywords, combined_data)

            filtered_data = pd.concat(filtered_data_list)
            data_12 = get_percentage_change_mentions_sentiment(
                existing_keywords, combined_data)

            display_users_page_visuals_layer_1(
                filtered_data, data_12, selected_keywords, existing_keywords)
            display_users_page_visuals_layer_2(
                filtered_data, data_12, selected_keywords, existing_keywords)
            display_user_page_visuals_networks(
                existing_keywords, selected_keywords, cursor)


def display_users_page_visuals_layer_1(archival_data: pd.DataFrame, data_upto_12hrs: pd.DataFrame, selected_keywords: list, existing_keywords: list):
    """Function to show visualisations of user's submitted keywords once logged in."""

    if len(selected_keywords) <= 2:
        left, right_1, right_2 = st.columns([4, 1, 1])

        if len(selected_keywords) == 0:
            last_submitted_keyword = existing_keywords[-1]
            keyword_id = fetch_keyword_id(
                last_submitted_keyword).get('keywords_id')
            archival_data = archival_data[archival_data['keywords_id']
                                          == keyword_id]

            data = data_upto_12hrs[data_upto_12hrs['keyword']
                                   == last_submitted_keyword]

            with left:
                st.markdown(
                    f'Your last submitted word was: **"{last_submitted_keyword}"**')
                chart = plot_total_mentions(
                    [last_submitted_keyword], archival_data)
                st.altair_chart(chart, use_container_width=True)
            with right_1:
                st.write("\n")
                get_total_mentions_change_metric(data)
            with right_2:
                st.write("\n")
                get_sentiment_overall_change_metric(data)
        elif len(selected_keywords) == 1:
            data = data_upto_12hrs[data_upto_12hrs['keyword']
                                   == existing_keywords[0]]
            with left:
                chart = plot_total_mentions(selected_keywords, archival_data)
                st.altair_chart(chart, use_container_width=True)
            with right_1:
                st.write("\n")
                get_total_mentions_change_metric(data)
            with right_2:
                st.write("\n")
                get_sentiment_overall_change_metric(data)
        elif len(selected_keywords) == 2:
            data_1 = data_upto_12hrs[data_upto_12hrs['keyword']
                                     == existing_keywords[0]]
            data_2 = data_upto_12hrs[data_upto_12hrs['keyword']
                                     == existing_keywords[1]]
            with left:
                chart = plot_total_mentions(selected_keywords, archival_data)
                st.altair_chart(chart, use_container_width=True)
            with right_1:
                st.markdown(
                    f'**{existing_keywords[0].title()}**')

                get_total_mentions_change_metric(data_1)
                get_sentiment_overall_change_metric(data_1)
            with right_2:
                st.markdown(
                    f'**{existing_keywords[1].title()}**')
                get_total_mentions_change_metric(data_2)
                get_sentiment_overall_change_metric(data_2)

    else:
        left, right, = st.columns([4, 2])
        with left:
            chart = plot_total_mentions(selected_keywords, archival_data)
            st.altair_chart(chart, use_container_width=True)

        with right:
            st.markdown("Since data collection began: ")
            table_data = add_keyword_column(selected_keywords, archival_data)
            result = table_data.groupby('keyword').apply(
                lambda group: pd.Series({
                    'total_mentions': round(group['total_mentions'].sum(), 2),
                    'average_sentiment': round(
                        (group['avg_sentiment'] * group['total_mentions']).sum() /
                        group['total_mentions'].sum(), 4)
                })
            )
            result['average_sentiment'] = result['average_sentiment'].apply(
                lambda x: f"{x:.2f}".rstrip('0').rstrip('.'))
            result['total_mentions'] = result['total_mentions'].apply(
                lambda x: f"{x:.0f}" if x.is_integer() else x)

            result.rename(columns={'total_mentions': 'Total Mentions',
                                   'average_sentiment': 'Average Sentiment'}, inplace=True)

            st.table(result)


def display_users_page_visuals_layer_2(archival_data: pd.DataFrame, data_upto_12hrs: pd.DataFrame, selected_keywords: list, existing_keywords: list):
    """Second layer of metrics and graphs to be displayed."""

    left, right = st.columns([2, 4])

    if len(selected_keywords) == 0:
        last_submitted_keyword = existing_keywords[-1]
        keyword_id = fetch_keyword_id(
            last_submitted_keyword).get('keywords_id')
        archival_data = archival_data[archival_data['keywords_id']
                                      == keyword_id]

        data = data_upto_12hrs[data_upto_12hrs['keyword']
                               == last_submitted_keyword]

        with left:
            st.markdown(
                f"In the next hour, we predict the total mentions of **{last_submitted_keyword}** to be:")
            prediction = main_predict(last_submitted_keyword)

            data = archival_data.sort_values(
                by=['date_and_hour'], ascending=False)
            only_keyword_data = data[data['keywords_id'] ==
                                     keyword_id]

            first_total_mentions = only_keyword_data['total_mentions'].iloc[0]
            delta = (prediction-first_total_mentions)

            st.metric(label='', value=prediction, delta=(
                prediction-first_total_mentions))
        with right:
            chart = plot_avg_sentiment_over_time(
                [last_submitted_keyword], archival_data)
            st.altair_chart(chart, use_container_width=True)
    elif len(selected_keywords) == 1:
        keyword_id = fetch_keyword_id(
            selected_keywords[0]).get('keywords_id')
        archival_data = archival_data[archival_data['keywords_id']
                                      == keyword_id]

        data = data_upto_12hrs[data_upto_12hrs['keyword']
                               == selected_keywords[0]]

        with left:
            st.markdown(
                f"In the next hour, we predict the total mentions of **{selected_keywords[0]}** to be:")
            prediction = main_predict(selected_keywords[0])
            data = archival_data.sort_values(
                by=['date_and_hour'], ascending=False)
            only_keyword_data = data[data['keywords_id'] ==
                                     keyword_id]

            first_total_mentions = only_keyword_data['total_mentions'].iloc[0]
            st.metric(label='', value=prediction, delta=(
                prediction-first_total_mentions))
        with right:
            chart = plot_avg_sentiment_over_time(
                [selected_keywords[0]], archival_data)
            st.altair_chart(chart, use_container_width=True)
    else:
        with left:
            keyword_predictions_list = []
            keyword_id_list = []
            for keyword in selected_keywords:
                keyword_predictions_list.append(
                    {'Keyword': keyword, 'Prediction': f"{main_predict(keyword):.2f}"})
                keyword_id_list.append(
                    fetch_keyword_id(keyword)['keywords_id'])

            st.markdown(
                f"In the next hour, we predict the total mentions of your keywords to be:")

            table_data = pd.DataFrame(
                keyword_predictions_list)

            data = archival_data.sort_values(
                by=['date_and_hour'], ascending=False)
            only_keyword_data = data[data['keywords_id'].isin(keyword_id_list)]

            first_total_mentions = only_keyword_data['total_mentions'].iloc[:(len(selected_keywords))].to_list(
            )
            table_data['Mentions in the last Hour'] = first_total_mentions
            table_data['Mentions in the last Hour'] = table_data['Mentions in the last Hour']
            table_data = table_data[['Keyword',
                                     'Mentions in the last Hour', 'Prediction']]

            st.table(table_data.set_index(['Keyword']))

        with right:
            chart = plot_avg_sentiment_over_time(
                selected_keywords, archival_data)
            st.altair_chart(chart, use_container_width=True)


def display_user_page_visuals_networks(existing_keywords, selected_keywords, cursor):
    """Display network graphs if only one or 2 keywords are present."""
    if len(selected_keywords) == 1:
        text, middle, _ = st.columns([1, 5, 1])
        with text:
            st.write("Explore the related terms: ")
        with middle:
            network_graph(selected_keywords[0], cursor)
    if len(selected_keywords) == 2:
        st.write("Explore the related terms:")
        graph1, _, graph2 = st.columns([7, 1, 7])
        with graph1:
            network_graph(selected_keywords[0], cursor)
        with graph2:
            network_graph(selected_keywords[1], cursor)


def network_graph(keyword, cursor):
    """Make a network graph for all the related terms of a given keyword"""
    nodes = []
    edges = []
    related_terms = {}

    result = get_related_words(keyword, cursor)
    related_terms[keyword] = [row.get('related_term') for row in result]

    added_node_ids = set()

    if keyword not in added_node_ids:
        nodes.append(Node(
            id=keyword,
            label=keyword,
            size=30,
            shape="circularImage",
            image="https://color-hex.org/colors/a3333d.png"
        ))
        added_node_ids.add(keyword)

    for idx, related_word in enumerate(related_terms[keyword]):
        if related_word not in added_node_ids:
            nodes.append(Node(
                id=related_word,
                label=related_word,
                size=30,
                shape="circularImage",
                image=COLOUR_IMAGES[idx % 2]
            ))
            added_node_ids.add(related_word)

        edges.append(Edge(
            source=keyword,
            label='',
            target=related_word
        ))

    config = Config(
        width=750,
        height=600,
        directed=True,
        physics=True,
        hierarchical=False,
    )

    return agraph(nodes=nodes, edges=edges, config=config)


def display_new_user_stats(cursor):
    """Function to display overall database values."""
    _, left, _, middle, _, right, _ = st.columns([2, 2, 1, 2, 1, 2, 1])
    with left:
        positive_word = get_most_positive_word(cursor)[0].get('keyword')

        sentiment = get_most_positive_word(cursor)[0].get('max_sentiment')
        st.metric(label='The most **positive** word is:',
                  value=positive_word, delta=sentiment)
    with middle:
        most_mentioned_word = get_most_mentioned_word(cursor)[0].get('keyword')
        mentions = get_most_mentioned_word(cursor)[0].get('total_mentions')
        st.metric('The most mentioned word is:',
                  value=most_mentioned_word, delta=mentions)
    with right:
        negative_word = get_most_negative_word(cursor)[0].get('keyword')
        sentiment = get_most_negative_word(cursor)[0].get('min_sentiment')
        st.metric(label='The most **negative** word is:',
                  value=negative_word, delta=sentiment)


def get_percentage_change_mentions_sentiment(keywords: list, data: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the percentage change over 12 hours in mention counts and sentiment changes.
    """
    filtered_data = add_keyword_column(keywords, data)
    filtered_data = filtered_data[filtered_data['keyword'].isin(
        keywords)].copy()

    now = pd.Timestamp.now()
    date_12_hours_ago = now - pd.Timedelta(hours=12)

    # Filter data for now and 12 hours ago
    now_data = filtered_data[filtered_data['date_and_hour'] <= now].sort_values(
        by='date_and_hour').groupby('keyword').tail(1)

    data_12_ago = filtered_data[
        filtered_data['date_and_hour'] <= date_12_hours_ago
    ].sort_values(by='date_and_hour').groupby('keyword').head(1)

    if data_12_ago.empty or now_data.empty:
        st.warning("Not enough historical data available.")
        return pd.DataFrame()

    merged_df = pd.merge(
        now_data[['keyword', 'avg_sentiment', 'total_mentions']],
        data_12_ago[['keyword', 'avg_sentiment', 'total_mentions']],
        on='keyword',
        suffixes=('_now', '_12_hrs_ago')
    )

    merged_df['percentage_change_mentions'] = (
        (merged_df['total_mentions_now'] - merged_df['total_mentions_12_hrs_ago']) /
        merged_df['total_mentions_12_hrs_ago']
    ) * 100

    merged_df['percentage_change_avg_sentiment'] = (
        (merged_df['avg_sentiment_now'] - merged_df['avg_sentiment_12_hrs_ago']) /
        merged_df['avg_sentiment_12_hrs_ago']
    ) * 100

    return merged_df[['keyword', 'avg_sentiment_12_hrs_ago', 'avg_sentiment_now',
                      'total_mentions_12_hrs_ago', 'total_mentions_now',
                      'percentage_change_mentions', 'percentage_change_avg_sentiment']]


def get_keyword_filter(existing_keywords: list, key: str = "keyword_filter") -> list:
    """Create a multiselect filter based on existing keywords."""
    return st.multiselect(
        'Choose keyword(s) to explore...',
        options=existing_keywords,
        default=existing_keywords[-3:],
        key=key,
        max_selections=5
    )


def filter_by_keyword(selected_keywords: list, data: pd.DataFrame) -> list:
    """Filter visualizations based on selected plant names."""
    if selected_keywords:
        keyword_ids = [fetch_keyword_id(keyword)["keywords_id"]
                       for keyword in selected_keywords]

        return [data[data['keywords_id'].isin(keyword_ids)]]

    return [data]


def add_keyword_column(keywords: list, data: pd.DataFrame) -> pd.DataFrame:
    """Adds a `keyword` column to the filtered data based on selected keywords."""
    keyword_id_map = {fetch_keyword_id(
        keyword)['keywords_id']: keyword for keyword in keywords}
    filtered_data = data[data['keywords_id'].isin(keyword_id_map.keys())]
    filtered_data['keyword'] = filtered_data['keywords_id'].map(keyword_id_map)
    return filtered_data


def plot_total_mentions(keywords: list, data: pd.DataFrame) -> alt.Chart:
    """Function to plot graph of total mentions of given keyword(s)."""

    filtered_data = add_keyword_column(keywords, data)

    chart = alt.Chart(filtered_data, title=f'Mentions Over Time').mark_line().encode(
        x=alt.X('date_and_hour:T', title='Date',
                axis=alt.Axis(format='%d-%m-%Y')),
        y=alt.Y('total_mentions:Q', title='Total Mentions'),
        color=alt.Color('keyword:N', title='Keyword:',
                        scale=alt.Scale(range=COLOUR_PALETTE)),
        tooltip=[alt.Tooltip('keyword:N', title='Keyword'),
                 alt.Tooltip('total_mentions:Q', title='Total Mentions'),
                 alt.Tooltip('date_and_hour:T', title='Date')]
    ).properties(width=800, height=400).interactive()
    return chart

# add an info box to explain average sentiment score meaning maybe add a link to vadersentiment / something to do with calc


def plot_avg_sentiment_over_time(keywords: list, data: pd.DataFrame) -> alt.Chart:
    """Function to plot graph of average sentiment overtime for a given keyword(s)."""
    filtered_data = add_keyword_column(keywords, data)

    chart = alt.Chart(filtered_data, title=f'Average Sentiment Over Time').mark_line().encode(
        x=alt.X('date_and_hour:T', title='Date',
                axis=alt.Axis(format='%d-%m-%Y')),
        y=alt.Y('avg_sentiment:Q', title='Average Sentiment'),
        color=alt.Color('keyword:N', title='Keyword:',
                        scale=alt.Scale(range=COLOUR_PALETTE)),
        tooltip=[alt.Tooltip('keyword:N', title='Keyword'),
                 alt.Tooltip('avg_sentiment:Q', title='Average Sentiment'),
                 alt.Tooltip('date_and_hour:T', title='Date')]
    ).properties(width=800, height=400).interactive()

    return chart


def get_sentiment_overall_change_metric(data: pd.DataFrame):
    """Function to show the metric displaying the overall change of average sentiment since the keyword being added into database."""
    if not data.empty and len(data) > 0:
        try:
            change = data.iloc[0]['percentage_change_avg_sentiment']
            change = change if not pd.isna(change) else 0
            now = data.iloc[0]['avg_sentiment_now']
            st.metric(label='Average Sentiment Now',
                      value=f'{now:.2f}', delta=f'{change:.2f}% over 12hrs')
        except Exception as e:
            st.error(f"Unexpected error accessing data: {e}")
            st.write("Data:", data)
    else:
        st.info("No data available to calculate sentiment change.")


def get_total_mentions_change_metric(data: pd.DataFrame):
    """Function to show metric displaying overall change in total mentions over 12 hours."""
    if not data.empty and len(data) > 0:
        try:
            change = data.iloc[0]['percentage_change_mentions']
            change = change if not pd.isna(change) else 0
            now = data.iloc[0]['total_mentions_now']

            st.metric(label='Total Mention Count Now',
                      value=f'{now}', delta=f'{change:.2f}% over 12hrs')
        except Exception as e:
            st.error(f"Unexpected error accessing data: {e}")
            st.write("Data:", data)
    else:
        st.info("No data available to calculate mentions change.")


if __name__ == "__main__":
    main()
