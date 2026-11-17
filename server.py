import socket
import threading
import json
import time

HOST = '127.0.0.1'
PORT = 5000

# Shared resources protected by a Lock (Resource Sharing challenge)
data_store = {}
active_nodes = {}
store_lock = threading.Lock()
request_count = 0


def handle_client(conn, addr):
    global request_count
    node_id = f"{addr[0]}:{addr[1]}"
    with store_lock:
        active_nodes[node_id] = time.time()

    print(f"[CONNECTED] Node {node_id} joined the distributed cluster.")

    try:
        while True:
            raw_data = conn.recv(4096)
            if not raw_data:
                break

            message = json.loads(raw_data.decode('utf-8'))
            action = message.get("action")

            with store_lock:
                request_count += 1
                req_num = request_count

            if action == "HEARTBEAT":
                with store_lock:
                    active_nodes[node_id] = time.time()
                response = {"status": "ACK", "msg": "Heartbeat received"}

            elif action == "PUT":
                key = message.get("key")
                value = message.get("value")
                with store_lock:
                    data_store[key] = value
                print(f"[REQ #{req_num}] {node_id} -> PUT ({key} = {value})")
                response = {"status": "OK", "msg": f"Stored '{key}' successfully"}

            elif action == "GET":
                key = message.get("key")
                with store_lock:
                    val = data_store.get(key, None)
                print(f"[REQ #{req_num}] {node_id} -> GET ({key}) => {val}")
                if val is not None:
                    response = {"status": "OK", "key": key, "value": val}
                else:
                    response = {"status": "NOT_FOUND", "msg": f"Key '{key}' not found"}

            elif action == "STATUS":
                with store_lock:
                    nodes_count = len(active_nodes)
                    keys_count = len(data_store)
                response = {
                    "status": "OK",
                    "active_nodes": nodes_count,
                    "stored_keys": keys_count,
                    "total_requests": req_num
                }

            else:
                response = {"status": "ERROR", "msg": "Unknown command"}

            conn.sendall(json.dumps(response).encode('utf-8'))

    except (ConnectionResetError, json.JSONDecodeError) as e:
        print(f"[FAILURE DETECTED] Node {node_id} disconnected unexpectedly: {e}")
    finally:
        with store_lock:
            active_nodes.pop(node_id, None)
        conn.close()
        print(f"[DISCONNECTED] Node {node_id} removed from cluster.")


def start_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(10)
    print(f"=== CS3551 Mini Distributed Coordinator Node running on {HOST}:{PORT} ===")
    print("Waiting for distributed client nodes to connect...\n")

    try:
        while True:
            conn, addr = server.accept()
            # Multithreading handles concurrent nodes (Scalability)
            thread = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
            thread.start()
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Coordinator server shutting down cleanly.")
    finally:
        server.close()


if __name__ == "__main__":
    start_server()
