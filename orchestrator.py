import os, time, requests
from kubernetes import client, config

# Load in-cluster config
config.load_incluster_config()
apps = client.AppsV1Api()
core = client.CoreV1Api()

NAMESPACE      = os.getenv("NAMESPACE", "default")
TARGET_DEPLOY  = os.getenv("TARGET_DEPLOYMENT", "lock-service")
INTERVAL       = int(os.getenv("SCALE_INTERVAL", "30"))

def fetch_rewards():
    pods = core.list_namespaced_pod(NAMESPACE, label_selector="app=lock-service").items
    rewards = []
    for p in pods:
        ip = p.status.pod_ip
        try:
            r = requests.get(f"http://{ip}:5000/metrics", timeout=1).json()
            rewards.append(r["cumulative_reward"])
        except:
            pass
    return rewards

while True:
    time.sleep(INTERVAL)
    rewards = fetch_rewards()
    if not rewards: continue
    avg = sum(rewards)/len(rewards)
    # count how many are above average
    top_count = sum(1 for r in rewards if r > avg)
    # ensure at least 1 replica
    new_replicas = max(1, top_count)
    # scale the deployment
    body = {"spec": {"replicas": new_replicas}}
    apps.patch_namespaced_deployment_scale(TARGET_DEPLOY, NAMESPACE, body)
    print(f"[Orch] set replicas={new_replicas} (avg_reward={avg:.1f})")
