"""Api.py: script setting up api to post new topics to."""

from flask import Flask, request, jsonify
from etl import main

app = Flask(__name__)


@app.route("/topics", methods=["POST"])
def add_topic()-> None:
    """API endpoint to add new topics to RDS."""
    data = request.get_json()
    topic_name = data.get("topic_name")

    if not topic_name:
        return jsonify({"message": "Topic name is required"}), 400

    main([topic_name])

    return jsonify({"message": "Topic added successfully", "topic": topic_name}), 200


if __name__ == "__main__":
    # app.run(host="0.0.0.0", port=5000, debug=True)
    app.run(debug=True)
