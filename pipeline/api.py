"""Api.py: script setting up api to post new topics to."""

from flask import Flask, request, jsonify
from load import main

app = Flask(__name__)

topics = []


@app.route("/topics", methods=["POST"])
def add_topic() -> None:
    """API endpoint to add new topics to RDS."""
    data = request.get_json()
    topic_name = data.get("topic_name")

    if not topic_name:
        return jsonify({"message": "Topic name is required"}), 400

    topic = {
        "topic_name": topic_name,
    }
    topics.append(topic)
    print([topic['topic_name']])
    main([topic_name])

    return jsonify({"message": "Topic added successfully", "topic": topic}), 200


@app.route("/topics", methods=["GET"])
def get_topics() -> None:
    """API endpoint to retrieve list of all posted topics."""
    return jsonify({"topics": topics}), 200


if __name__ == "__main__":
    # app.run(host="0.0.0.0", port=5000, debug=True)
    app.run(debug=True)
