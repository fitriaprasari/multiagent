from flask import Flask, request, jsonify
import threading, time

app = Flask(__name__)
balances = {}
row_locks = {}
metrics_lock = threading.Lock()
# each pod will keep its own cumulative_reward in memory
cumulative_reward = 0.0
last_collision = {}

BETA = float(request.environ.get("BETA", 5.0))
GAMMA = float(request.environ.get("GAMMA", 0.3))
DELTA_MS = int(request.environ.get("DELTA_MS", 10))

@app.route("/accounts/<int:acct>/lock", methods=["POST"])
def lock(acct):
    global cumulative_reward, last_collision
    start = time.time()
    lock = row_locks.setdefault(acct, threading.Lock())
    # memory-1 TFT back-off
    if last_collision.get(acct, False):
        time.sleep(DELTA_MS / 1000.0)
    acquired = lock.acquire(blocking=False)
    rollback = 0
    if acquired:
        # simulate a quick write
        balances[acct] = balances.get(acct, 0) + request.json.get("amount", 0)
        time.sleep(0.005)
        lock.release()
    else:
        rollback = 1
    last_collision[acct] = not acquired
    latency_ms = int((time.time() - start) * 1000)

    # responsibility-aware reward
    # (in real deployment you'd fetch peer avg from a metrics store)
    avg_rb = float(request.environ.get("AVG_ROLLBACK", 0))
    reward = -latency_ms - BETA * rollback - GAMMA * avg_rb
    with metrics_lock:
        cumulative_reward += reward

    return jsonify(result="OK" if acquired else "COLLISION", latency_ms=latency_ms)

@app.route("/accounts/<int:acct>/balance", methods=["GET"])
def balance(acct):
    start = time.time()
    # simply return current balance
    bal = balances.get(acct, 0)
    latency_ms = int((time.time() - start) * 1000)
    return jsonify(balance=bal, latency_ms=latency_ms)

@app.route("/metrics", methods=["GET"])
def metrics():
    # expose cumulative_reward for orchestrator
    with metrics_lock:
        cr = cumulative_reward
    return jsonify(cumulative_reward=cr)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
