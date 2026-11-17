import socket
import json
import time
import threading

HOST = '127.0.0.1'
PORT = 5000
MAX_RETRIES = 3


def connect_with_retry():
    """Demonstrates Failure Handling via exponential/fixed retry logic."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect((HOST, PORT))
            print(f"[CONNECTED] Connected to Coordinator at {HOST}:{PORT}")
            return sock
        except (ConnectionRefusedError, socket.timeout) as e:
            print(f"[RETRY {attempt}/{MAX_RETRIES}] Server unreachable ({e}). Retrying in 2s...")
            time.sleep(2)
    return None


def send_heartbeat(sock, stop_event):
    """Background thread sending periodic heartbeats to maintain liveness."""
    while not stop_event.is_set():
        time.sleep(10)
        try:
            msg = json.dumps({"action": "HEARTBEAT"}).encode('utf-8')
            sock.sendall(msg)
        except Exception:
            break


def run_client():
    sock = connect_with_retry()
    if not sock:
        print("[FATAL] Could not connect to distributed coordinator after retries.")
        return

    stop_event = threading.Event()
    hb_thread = threading.Thread(target=send_heartbeat, args=(sock, stop_event), daemon=True)
    hb_thread.start()

    print("\n--- Distributed Node Commands ---")
    print("1. PUT <key> <value>  (Write to shared distributed store)")
    print("2. GET <key>          (Read from shared distributed store)")
    print("3. STATUS             (Check cluster scalability & node count)")
    print("4. EXIT               (Leave cluster)\n")

    try:
        while True:
            cmd_input = input("Node> ").strip()
            if not cmd_input:
                continue

            parts = cmd_input.split(" ", 2)
            cmd = parts[0].upper()

            if cmd == "EXIT":
                break
            elif cmd == "PUT" and len(parts) == 3:
                payload = {"action": "PUT", "key": parts[1], "value": parts[2]}
            elif cmd == "GET" and len(parts) == 2:
                payload = {"action": "GET", "key": parts[1]}
            elif cmd == "STATUS":
                payload = {"action": "STATUS"}
            else:
                print("Invalid command. Use: PUT <key> <value> | GET <key> | STATUS | EXIT")
                continue

            sock.sendall(json.dumps(payload).encode('utf-8'))
            reply = json.loads(sock.recv(4096).decode('utf-8'))
            print("Server Response:", reply)

    except (BrokenPipeError, ConnectionResetError, socket.timeout):
        print("[ERROR] Connection lost! Coordinator node failed or timed out.")
    finally:
        stop_event.set()
        sock.close()
        print("[CLOSED] Client node terminated.")


if __name__ == "__main__":
    run_client()
