import threading
import queue
import random
import time

# Simulated PostgreSQL cluster with row-level locks
class PostgresCluster:
    def __init__(self, n_rows=1):
        self.locks = {i: threading.Lock() for i in range(n_rows)}

    def attempt_lock(self, row_id):
        """Try to acquire lock non-blocking. Return True on success."""
        lock = self.locks[row_id]
        return lock.acquire(blocking=False)

    def release_lock(self, row_id):
        """Release the acquired lock."""
        self.locks[row_id].release()

# Service Node representing a microservice pod
class ServiceNode(threading.Thread):
    id_counter = 0

    def __init__(self, name, request_queue, db, orchestrator,
                 beta=5, gamma=0.3, delta_ms=10):
        super().__init__(daemon=True)
        ServiceNode.id_counter += 1
        self.name = f"{name}_{ServiceNode.id_counter}"
        self.request_queue = request_queue
        self.db = db
        self.orch = orchestrator
        self.beta = beta
        self.gamma = gamma
        self.delta_ms = delta_ms
        self.last_collision = False
        self.cum_reward = 0.0
        self.running = True

    def run(self):
        while self.running:
            try:
                row_id = self.request_queue.get(timeout=1)
            except queue.Empty:
                continue

            # Memory-1 TFT strategy: back-off if last round was collision
            if self.last_collision:
                time.sleep(self.delta_ms / 1000.0)

            start = time.time()
            success = self.db.attempt_lock(row_id)
            latency = (time.time() - start) * 1000  # ms
            rollback = 0

            if success:
                # Simulate quick transaction work
                time.sleep(0.005)
                self.db.release_lock(row_id)
            else:
                rollback = 1

            self.last_collision = not success

            # Compute Responsibility-Aware Reward
            reward = -latency - self.beta * rollback
            avg_rb = self.orch.get_avg_rollbacks()
            reward -= self.gamma * avg_rb
            self.cum_reward += reward

            # Report rollback outcome to orchestrator
            self.orch.report_rollback(rollback)

# Orchestrator implementing discrete‐time replicator dynamics
class Orchestrator(threading.Thread):
    def __init__(self, services, interval=10):
        super().__init__(daemon=True)
        self.services = services
        self.interval = interval
        self.rollback_counts = []
        self.lock = threading.Lock()

    def report_rollback(self, count):
        with self.lock:
            self.rollback_counts.append(count)

    def get_avg_rollbacks(self):
        with self.lock:
            return sum(self.rollback_counts) / len(self.rollback_counts) if self.rollback_counts else 0

    def run(self):
        while True:
            time.sleep(self.interval)
            # Sort by cumulative reward (highest first)
            self.services.sort(key=lambda s: s.cum_reward, reverse=True)
            n = len(self.services)
            top_k = max(1, n // 5)  # top 20%
            bot_k = top_k           # bottom 20%

            # Scale down bottom performers
            for svc in self.services[-bot_k:]:
                svc.running = False
            # Keep only survivors
            self.services = self.services[:-bot_k]

            # Scale up top performers by cloning
            clones = []
            for i in range(top_k):
                parent = self.services[i]
                clone = ServiceNode(
                    name=parent.name.split("_")[0] + "_clone",
                    request_queue=parent.request_queue,
                    db=parent.db,
                    orchestrator=self,
                    beta=parent.beta,
                    gamma=parent.gamma,
                    delta_ms=parent.delta_ms
                )
                clone.start()
                clones.append(clone)

            # Add clones to service pool
            self.services.extend(clones)

            # Reset rollback tracking
            with self.lock:
                self.rollback_counts.clear()

# Transaction client generating lock requests
class TransactionClient(threading.Thread):
    def __init__(self, request_queue, rate=50, n_rows=1):
        super().__init__(daemon=True)
        self.request_queue = request_queue
        self.rate = rate  # requests per second
        self.n_rows = n_rows

    def run(self):
        while True:
            time.sleep(1.0 / self.rate)
            row_id = random.randrange(self.n_rows)
            self.request_queue.put(row_id)

# Main simulation entry point
if __name__ == "__main__":
    NUM_SERVICES = 3
    NUM_ROWS = 1
    REQUEST_RATE = 50     # requests per second
    DURATION = 60         # simulation time in seconds
    SCALE_INTERVAL = 10   # orchestrator interval in seconds

    # Shared queue and database
    request_queue = queue.Queue()
    db = PostgresCluster(n_rows=NUM_ROWS)

    # Initialize service instances
    services = []
    for i in range(NUM_SERVICES):
        svc = ServiceNode(
            name=f"svc{i}",
            request_queue=request_queue,
            db=db,
            orchestrator=None
        )
        services.append(svc)

    # Start orchestrator
    orchestrator = Orchestrator(services, interval=SCALE_INTERVAL)
    # Inject orchestrator reference into services
    for svc in services:
        svc.orch = orchestrator
    orchestrator.start()

    # Start all service threads
    for svc in services:
        svc.start()

    # Start transaction client
    client = TransactionClient(request_queue, rate=REQUEST_RATE, n_rows=NUM_ROWS)
    client.start()

    # Run the simulation for a specified duration
    time.sleep(DURATION)
    print("Simulation complete. Final service count:", len(orchestrator.services))

