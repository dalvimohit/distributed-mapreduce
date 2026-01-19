import os
import requests
import subprocess
import time
import json

KV_URL = "http://127.0.0.1:7000"
NUM_MAPPERS = 4
NUM_REDUCERS = 3
BASE_WORKER_PORT = 6001
INPUT_FOLDER = "input_data"

def kv_put(key, value):
    requests.post(f"{KV_URL}/put", json={"key": key, "value": value})

def kv_get(key):
    try:
        resp = requests.get(f"{KV_URL}/get/{key}")
        if resp.status_code == 200:
            return resp.json().get("value")
        return None
    except:
        return None

# ---------------- Prepare Input ----------------
chunks = []
files = sorted(os.listdir(INPUT_FOLDER))
for f in files:
    with open(os.path.join(INPUT_FOLDER, f)) as file:
        chunks.append(file.read())

# Split chunks according to NUM_MAPPERS
if len(chunks) < NUM_MAPPERS:
    # Split large chunks into smaller pieces
    total_text = "\n".join(chunks)
    lines = total_text.split("\n")
    chunk_size = len(lines) // NUM_MAPPERS + 1
    chunks = ["\n".join(lines[i*chunk_size:(i+1)*chunk_size]) for i in range(NUM_MAPPERS)]
elif len(chunks) > NUM_MAPPERS:
    # Merge extra files into existing chunks
    merged = [""] * NUM_MAPPERS
    for idx, chunk in enumerate(chunks):
        merged[idx % NUM_MAPPERS] += chunk + "\n"
    chunks = merged

# PUT chunks in KV
for i, chunk in enumerate(chunks):
    kv_put(f"chunk_{i}", chunk)
    print(f"PUT chunk_{i}")

# ---------------- SPAWN WORKERS ----------------
addresses = []
for i in range(max(NUM_MAPPERS, NUM_REDUCERS)):
    port = BASE_WORKER_PORT + i
    subprocess.Popen(["python", "worker.py", str(port)])
    addresses.append(f"http://127.0.0.1:{port}")

time.sleep(2)

# ---------------- MAP PHASE ----------------
print("Starting MAP phase...")
for i in range(NUM_MAPPERS):
    worker_addr = addresses[i % len(addresses)]
    print(f"Sending map task for chunk_{i} to {worker_addr}")
    requests.post(f"{worker_addr}/command", json={
        "task_type": "map",
        "chunk_id": f"chunk_{i}"
    })

# Wait until all map tasks are done
for i in range(NUM_MAPPERS):
    while kv_get(f"chunk_{i}_done") != "true":
        time.sleep(0.5)
    print(f"chunk_{i} done ✅")

print("All MAP tasks completed ✅")

# ---------------- REDUCE PHASE ----------------
print("Starting REDUCE phase...")
for i in range(NUM_REDUCERS):
    worker_addr = addresses[i % len(addresses)]
    requests.post(f"{worker_addr}/command", json={
        "task_type": "reduce",
        "reducer_id": i
    })

# Wait for all reducers to finish
for i in range(NUM_REDUCERS):
    while kv_get(f"final_output_{i}_done") != "true":
        time.sleep(0.5)

# ---------------- MERGE RESULTS ----------------
final_word_count = {}
for i in range(NUM_REDUCERS):
    reducer_output = kv_get(f"final_output_{i}")
    if reducer_output:
        final_word_count.update(json.loads(reducer_output))

print("\n✅ MapReduce job completed. Final word counts:")
for word, count in sorted(final_word_count.items()):
    print(f"{word}: {count}")










