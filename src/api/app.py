"""
api/app.py
Minimal Flask stub for Milestone 1.
Keeps the app container alive without crashing.
Full implementation in Milestone 4.
"""

from flask import Flask, jsonify

app = Flask(__name__)


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "milestone": 1}), 200


@app.route("/recommend", methods=["POST"])
def recommend():
    return jsonify({
        "message": "Recommendation engine not yet implemented.",
        "milestone": "Implement in Milestone 4"
    }), 501


if __name__ == "__main__":
    import os
    port = int(os.getenv("FLASK_PORT", 5050))
    app.run(host="0.0.0.0", port=port)