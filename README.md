# CS3551 – Distributed Computing (Mini Project)
**Topic 5:** Design a mini distributed application and analyze its major design issues and challenges, including scalability, communication, failure handling, and resource sharing.

## Project Structure
- `server.py` – Central Coordinator Node handling concurrent client connections, thread-safe resource sharing (`threading.Lock`), and failure detection.
- `client_node.py` – Interactive Distributed Client Node with retry-based failure handling, background heartbeats, and JSON-over-TCP communication.
- `test_simulation.py` – Automated test script that spawns 5 concurrent simulated nodes to verify scalability and synchronization.

## How to Run (Requires Python 3.x)
1. **Start the Coordinator Server** (Terminal 1):
   ```bash
   python server.py
   ```
2. **Start One or More Client Nodes** (Terminal 2, Terminal 3, etc.):
   ```bash
   python client_node.py
   ```
   Try commands:
   - `PUT user1 TamilNadu`
   - `GET user1`
   - `STATUS`
   - `EXIT`
3. **Run Automated Multi-Node Test** (while `server.py` is running):
   ```bash
   python test_simulation.py
   ```

## Design Challenges Demonstrated
1. **Scalability:** Handled using multithreaded socket connections (`threading.Thread`) so multiple nodes can connect concurrently.
2. **Communication:** Implemented using structured JSON message passing over reliable TCP sockets.
3. **Failure Handling:** Implemented using connection retry loops (`MAX_RETRIES`), socket timeouts, `try-except` disconnect detection, and periodic `HEARTBEAT` messages.
4. **Resource Sharing:** Protected the shared in-memory key-value dictionary and request counter with mutual exclusion locks (`threading.Lock`) to prevent race conditions.
