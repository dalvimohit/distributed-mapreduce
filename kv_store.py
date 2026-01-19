from flask import Flask, request, jsonify

app = Flask(__name__)

store = {}

@app.route("/put", methods=["POST"])
def put():
    data = request.json
    key = data.get("key")
    value = data.get("value")
    if key is None or value is None:
        return jsonify({"status": "error", "message": "key or value missing"}), 400

    store[key] = value
    return jsonify({"status": "ok"}), 200

@app.route("/get/<key>", methods=["GET"])
def get(key):
    value = store.get(key)
    if value is None:
        return jsonify({"status": "error", "message": "key not found"}), 404
    return jsonify({"status": "ok", "value": value}), 200

@app.route("/delete/<key>", methods=["POST"])
def delete(key):
    if key in store:
        del store[key]
        return jsonify({"status": "ok"}), 200
    return jsonify({"status": "error", "message": "key not found"}), 404

@app.route("/list", methods=["GET"])
def list_keys():
    return jsonify({"keys": list(store.keys())}), 200

if __name__ == "__main__":
    print("KV Store running on port 7000")
    app.run(port=7000)



