#!/usr/bin/env python3
"""
Enhanced Namenode for Mini-HDFS
Handles:
- Heartbeats from Datanodes
- Metadata management
- Chunk allocation and replica coordination
- Node health monitoring and re-replication
"""

import os, json, socket, time, threading, struct
from flask import Flask, jsonify, request, render_template_string

# === Configuration ===
HEARTBEAT_PORT = 9000
API_PORT = 8000
HB_TIMEOUT = 8          # seconds before node considered dead
TARGET_REPLICAS = 2     # desired replication factor
META_DIR = "namenode_store"
METADATA_PATH = os.path.join(META_DIR, "files_metadata.json")
DATANODES_PATH = os.path.join(META_DIR, "datanode_status.json")

os.makedirs(META_DIR, exist_ok=True)
for p in [METADATA_PATH, DATANODES_PATH]:
    if not os.path.exists(p):
        with open(p, "w") as f:
            json.dump({}, f)

# Thread lock for safe access
mutex = threading.Lock()

# === Helper functions ===
def read_json_file(path):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}

def save_json_file(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2, sort_keys=True)

def send_message(conn, payload):
    data = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    conn.sendall(struct.pack("!I", len(data)) + data)

def receive_message(conn):
    raw = conn.recv(4)
    if not raw:
        return None
    (length,) = struct.unpack("!I", raw)
    data = b""
    while len(data) < length:
        chunk = conn.recv(length - len(data))
        if not chunk:
            break
        data += chunk
    return data if len(data) == length else None

# === Load stored Datanodes ===
try:
    datanodes = read_json_file(DATANODES_PATH)
    if not isinstance(datanodes, dict):
        datanodes = {}
except Exception:
    datanodes = {}

# === Heartbeat Handling ===
def handle_heartbeat(conn, addr):
    """Process single heartbeat connection from Datanode"""
    try:
        msg = receive_message(conn)
        if not msg:
            return
        data = json.loads(msg.decode())
        node_id = str(data.get("id"))
        with mutex:
            node_info = dict(data)
            node_info["ip"] = data.get("host") or addr[0]
            node_info["alive"] = True
            node_info["last_seen"] = time.time()
            datanodes[node_id] = node_info
            save_json_file(DATANODES_PATH, datanodes)
        print(f"[HB] Received heartbeat from {node_id} @ {node_info['ip']}:{node_info.get('port')} (now alive)")
        send_message(conn, b"OK")
    except Exception as e:
        print("Heartbeat error:", e)
    finally:
        conn.close()

def heartbeat_server():
    """Start listening for heartbeats"""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", HEARTBEAT_PORT))
    srv.listen(5)
    print(f"[Namenode] Listening for heartbeats on port {HEARTBEAT_PORT}")
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle_heartbeat, args=(conn, addr), daemon=True).start()

# === Health Monitoring and Re-replication ===
def node_monitor():
    """Continuously check node liveness and re-replicate if needed"""
    while True:
        time.sleep(2)
        now = time.time()
        nodes_to_fix = []
        updated = False
        with mutex:
            for node_id, info in datanodes.items():
                alive = (now - info.get("last_seen", 0)) <= HB_TIMEOUT
                if info.get("alive") != alive:
                    datanodes[node_id]["alive"] = alive
                    status = "active" if alive else "inactive"
                    print(f"[Monitor] Node {node_id} marked {status}")
                    updated = True
                    if not alive:
                        nodes_to_fix.append(node_id)
            if updated:
                save_json_file(DATANODES_PATH, datanodes)
        for nid in nodes_to_fix:
            try:
                handle_rereplication(nid)
            except Exception as e:
                print("Re-replication failed:", e)

def select_replicas(fname, index, count=TARGET_REPLICAS):
    """Pick alive nodes for chunk replicas"""
    with mutex:
        active = [(nid, info) for nid, info in datanodes.items() if info.get("alive")]
        active.sort(key=lambda x: x[0])
        chosen = []
        for nid, info in active:
            if info.get("ip") and info.get("port"):
                chosen.append({"id": nid, "host": info["ip"], "port": info["port"]})
            if len(chosen) >= count:
                break
        return chosen

# === Flask API ===
app = Flask(__name__)

@app.route("/")
def dashboard():
    files = read_json_file(METADATA_PATH)
    nodes = read_json_file(DATANODES_PATH)
    template = """
    <!doctype html><title>Namenode Monitor</title>
    <h1>Namenode Status Dashboard</h1>
    <h2>Datanode Health</h2>
    <table border=1 cellpadding=5>
      <tr><th>ID</th><th>Host</th><th>Port</th><th>Alive</th><th>Last Seen (sec ago)</th></tr>
      {% for id, info in nodes.items() %}
        <tr>
          <td>{{id}}</td><td>{{info.get('ip')}}</td><td>{{info.get('port')}}</td><td>{{info.get('alive')}}</td>
          <td>{% if info.get('last_seen') %}{{('%.1f' % (now - info['last_seen']))}}{% else %}N/A{% endif %}</td>
        </tr>
      {% endfor %}
    </table>
    <h2>Files Metadata</h2>
    <pre>{{files|tojson(indent=2)}}</pre>
    """
    return render_template_string(template, files=files, nodes=nodes, now=time.time())

@app.route("/allocate_chunk", methods=["POST"])
def allocate_chunk():
    data = request.get_json()
    fname = data["filename"]
    idx = int(data["chunk_index"])
    replicas = select_replicas(fname, idx)
    print(f"[Allocate] Chunk {fname}#{idx} -> {replicas}")
    return jsonify({"replicas": replicas})

@app.route("/commit_file", methods=["POST"])
def commit_file():
    body = request.get_json()
    fname = body["filename"]
    with mutex:
        meta = read_json_file(METADATA_PATH)
        meta[fname] = {
            "filesize": body.get("filesize"),
            "chunks": body["chunks"],
            "uploaded_at": time.time()
        }
        save_json_file(METADATA_PATH, meta)
    print(f"[Meta] File {fname} committed with {len(body['chunks'])} chunks.")
    return jsonify({"status": "ok"})

@app.route("/list_files", methods=["GET"])
def list_files():
    meta = read_json_file(METADATA_PATH)
    return jsonify(list(meta.keys()))

@app.route("/file_info/<path:fname>", methods=["GET"])
def file_info(fname):
    meta = read_json_file(METADATA_PATH)
    if fname not in meta:
        return jsonify({"error": "file not found"}), 404
    return jsonify(meta[fname])

@app.route("/datanodes", methods=["GET"])
def datanode_info():
    return jsonify(read_json_file(DATANODES_PATH))

# === Re-replication ===
def handle_rereplication(dead_node):
    print(f"[ReReplica] Initiating re-replication for {dead_node}")
    with mutex:
        files = read_json_file(METADATA_PATH)
        nodes = read_json_file(DATANODES_PATH)
        alive = {nid: info for nid, info in nodes.items() if info.get("alive")}
    for fname, details in files.items():
        chunks = details.get("chunks", [])
        modified = False
        for ch in chunks:
            replica_ids = [r.get("id") for r in ch.get("replicas", [])]
            if dead_node in replica_ids:
                src, dest = None, None
                for r in ch["replicas"]:
                    if r["id"] != dead_node and r["id"] in alive:
                        src = r
                        break
                for nid, info in alive.items():
                    if nid not in replica_ids:
                        dest = {"id": nid, "host": info["ip"], "port": info["port"]}
                        break
                if src and dest:
                    print(f"[ReReplica] {fname}#{ch['index']} → copy from {src['id']} to {dest['id']}")
                    try:
                        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        s.settimeout(8)
                        s.connect((src["host"], int(src["port"])))
                        req = {"cmd": "send_chunk_to", "meta": {"filename": fname,
                                "chunk_index": ch["index"], "dest_host": dest["host"], "dest_port": int(dest["port"])}}
                        send_message(s, json.dumps(req).encode())
                        raw = receive_message(s)
                        if not raw:
                            print("[ReReplica] No reply from source node")
                        else:
                            reply = json.loads(raw.decode())
                            if reply.get("status") == "ok":
                                ch["replicas"].append(dest)
                                modified = True
                                print(f"[ReReplica] {fname}#{ch['index']} replicated successfully to {dest['id']}")
                            else:
                                print(f"[ReReplica] Source error: {reply}")
                        s.close()
                    except Exception as e:
                        print("ReReplica network issue:", e)
                else:
                    print(f"[ReReplica] Skipping {fname}#{ch['index']} — no available source/target.")
        if modified:
            with mutex:
                save_json_file(METADATA_PATH, files)
    print(f"[ReReplica] Completed for {dead_node}")

# === Start Servers ===
if __name__ == "__main__":
    # Note: thread order changed intentionally
    print("[Namenode] Starting main services...")
    threading.Thread(target=node_monitor, daemon=True).start()
    threading.Thread(target=heartbeat_server, daemon=True).start()
    app.run(host="0.0.0.0", port=API_PORT, threaded=True)
