from flask import Flask, jsonify
from flask_cors import CORS
import json
from pathlib import Path

app = Flask(__name__, static_folder="../frontend", static_url_path="")
CORS(app)

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "outputs"

@app.route("/")
def index():
    return app.send_static_file("index.html")

@app.route("/api/analytics")
def get_analytics():
    path = OUT_DIR / "analytics.json"
    if path.exists():
        return jsonify(json.loads(path.read_text(encoding="utf-8")))
    return jsonify({"error": "Analytics data not found"}), 404

@app.route("/api/recommendations/<int:user_id>")
def get_recommendations(user_id):
    path = OUT_DIR / "recommendations.json"
    if path.exists():
        data = json.loads(path.read_text(encoding="utf-8"))
        # JSON keys are always strings
        str_id = str(user_id)
        if str_id in data:
            return jsonify(data[str_id])
        return jsonify({"error": "User not found"}), 404
    return jsonify({"error": "Recommendations data not found"}), 404

@app.route("/api/users")
def get_users():
    path = OUT_DIR / "recommendations.json"
    if path.exists():
        data = json.loads(path.read_text(encoding="utf-8"))
        users = sorted([int(k) for k in data.keys()])
        return jsonify(users)
    return jsonify({"error": "Recommendations data not found"}), 404

if __name__ == "__main__":
    app.run(debug=True, port=5000)
