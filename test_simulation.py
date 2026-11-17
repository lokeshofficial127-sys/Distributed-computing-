import socket
import threading
import json
import time

HOST = '127.0.0.1'
PORT = 5000


def simulated_worker(worker_id):
    """Simulates concurrent distributed nodes accessing shared resources."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((HOST, PORT))

        # Write shared key
        put_msg = json.dumps({"action": "PUT", "key": f"node_{worker_id}_status", "value": "ACTIVE"})
        sock.sendall(put_msg.encode('utf-8'))
        res1 = json.loads(sock.recv(4096).decode('utf-8'))

        # Read shared counter/key
        get_msg = json.dumps({"action": "GET", "key": f"node_{worker_id}_status"})
        sock.sendall(get_msg.encode('utf-8'))
        res2 = json.loads(sock.recv(4096).decode('utf-8'))

        print(f"[Worker-{worker_id}] PUT -> {res1['status']} | GET -> {res2.get('value')}")
        sock.close()
    except Exception as e:
        print(f"[Worker-{worker_id}] Error: {e}")


if __name__ == "__main__":
    print("=== Running Automated 5-Node Distributed Simulation ===")
    threads = []
    for i in range(1, 6):
        t = threading.Thread(target=simulated_worker, args=(i,))
        threads.append(t)
        t.start()
        time.sleep(0.1)

    for t in threads:
        t.join()

    print("=== Simulation Complete ===")
