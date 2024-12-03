from flask import Flask, request, jsonify
from load import main

app = Flask(__name__)

topics = []


@app.route("/topics", methods=["POST"])
def add_topic():
    data = request.get_json()
    topic_name = data.get("topic_name")
    notification_threshold = data.get("notification_threshold", 0)
    user_name = data.get("user_name")
    phone_number = data.get("phone_number")
    subscription_status = data.get("subscription_status", "disabled").lower()

    if not topic_name or not user_name or not phone_number:
        return jsonify({"message": "Topic name, user name, and phone number are required"}), 400

    if subscription_status not in ["enabled", "disabled"]:
        return jsonify({"message": "Invalid subscription status. Must be 'enabled' or 'disabled'"}), 400

    topic = {
        "topic_name": topic_name,
        "notification_threshold": notification_threshold,
        "user_name": user_name,
        "phone_number": phone_number,
        "subscription_status": subscription_status,
    }
    topics.append(topic)
    print(topic)
    # main(topic_name)

    return jsonify({"message": "Topic added successfully", "topic": topic}), 200


@app.route("/topics", methods=["GET"])
def get_topics():
    return jsonify({"topics": topics}), 200


if __name__ == "__main__":
    # app.run(host="0.0.0.0", port=5000, debug=True)
    app.run(debug=True)
