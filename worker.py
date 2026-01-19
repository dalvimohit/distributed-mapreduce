import sys
import requests
import time
import json
from flask import Flask, request

app = Flask(__name__)
KV_URL = "http://127.0.0.1:7000"

PORT = int(sys.argv[1])

def kv_put(key, value):
    if isinstance(value, dict):
        value = json.dumps(value)
    requests.post(f"{KV_URL}/put", json={"key": key, "value": value})

def kv_get(key):
    try:
        resp = requests.get(f"{KV_URL}/get/{key}")
        if resp.status_code == 200:
            return resp.json().get("value")
        return None
    except:
        return None

def map_task(chunk_id):
    data = kv_get(chunk_id)
    word_count = {}
    if data:
        for word in data.replace("\n", " ").split():
            word_count[word.lower()] = word_count.get(word.lower(), 0) + 1
    kv_put(f"intermediate_{chunk_id}", word_count)
    kv_put(f"{chunk_id}_done", "true")
    print(f"Worker on port {PORT} finished MAP for {chunk_id}")

def reduce_task(reducer_id, num_mappers):
    final_count = {}
    # Get all intermediate results
    for i in range(num_mappers):
        intermediate = kv_get(f"intermediate_chunk_{i}")
        if intermediate:
            intermediate = json.loads(intermediate)
            for k, v in intermediate.items():
                final_count[k] = final_count.get(k, 0) + v
    kv_put(f"final_output_{reducer_id}", final_count)
    kv_put(f"final_output_{reducer_id}_done", "true")
    print(f"Worker on port {PORT} finished REDUCE {reducer_id}")

@app.route("/command", methods=["POST"])
def command():
    cmd = request.json
    if cmd["task_type"] == "map":
        chunk_id = cmd["chunk_id"]
        map_task(chunk_id)
    elif cmd["task_type"] == "reduce":
        reducer_id = cmd["reducer_id"]
        reduce_task(reducer_id, NUM_MAPPERS)
    return "ok"

if __name__ == "__main__":
    NUM_MAPPERS = 4  # hardcoded for assignment
    app.run(port=PORT)














