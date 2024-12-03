from os import environ as ENV
from dotenv import load_dotenv
import streamlit as st
import requests

load_dotenv()

API_ENDPOINT = ENV["API_ENDPOINT"]

st.title("Trend Getter")
st.write("Submit a topic to track trends along with your details.")

with st.form("topic_form"):
    topic_name = st.text_input("Enter the topic or keyword:")
    notification_threshold = st.number_input(
        "Set notification threshold (optional):", min_value=0, value=0)
    user_name = st.text_input("Enter your name:")
    phone_number = st.text_input("Enter your phone number:")
    subscription_status = st.selectbox(
        "Subscription Status:", ["enabled", "disabled"])
    submit_button = st.form_submit_button("Submit")

if submit_button:
    if topic_name.strip() and user_name.strip() and phone_number.strip():
        data = {
            "topic_name": topic_name.strip(),
            "notification_threshold": notification_threshold,
            "user_name": user_name.strip(),
            "phone_number": phone_number.strip(),
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
        st.warning("Please enter all required fields.")
