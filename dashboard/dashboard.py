from os import environ as ENV
from dotenv import load_dotenv
import streamlit as st
import requests

load_dotenv()

API_ENDPOINT = ENV["API_ENDPOINT"]

st.title("Trend Getter")
st.write("Submit a topic to track trends.")

with st.form("topic_form"):
    topic_name = st.text_input("Enter the topic or keyword:")
    print(topic_name)
    notification_threshold = st.number_input(
        "Set notification threshold (optional):", min_value=0, value=0)
    submit_button = st.form_submit_button("Submit")

if submit_button:
    if topic_name.strip():
        data = {
            "topic_name": topic_name.strip(),
            "notification_threshold": notification_threshold,
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
