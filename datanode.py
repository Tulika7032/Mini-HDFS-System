#!/usr/bin/env python3
"""
MiniHDFS Datanode:
- Sends heartbeats to Namenode
- Stores and serves file chunks
- Supports sending chunks to other datanodes (re-replication)
"""

"""
Datanode-0 (Storage Node A)
Responsibilities:
1. Chunk reception & storage
2. Heartbeat sender thread & communication stability
3. Retrieval logic for client downloads
4. Data integrity checks (no corruption on retrieval)
5. Logging & error handling
"""

"""
Datanode-1 (Storage Node B)
Responsibilities:
1. Chunk replication & verification
2. Heartbeat sender thread & connectivity
3. Serving replica chunks to client
4. Data validation & checksum consistency
5. Robustness & recovery during node failure tests
"""

import argparse
import os
import socket
import struct
import threading
import time
import json
import hashlib
from pathlib import Path

# --------------------- Utility Functions ---------------------
def compute_sha256(data_bytes):
    return hashlib.sha256(data_bytes).hexdigest()

def format_bytes(size_bytes):
    return f"{size_bytes / 1024:.2f} KB"

def get_folder_size(folder_path):
    total = 0
    folder = Path(folder_path)
    for f in folder.rglob("*"):
        if f.is_file():
            total += f.stat().st_size
    return total

# --------------------- Networking Helpers ---------------------
def send_packet(sock, payload_bytes):
    header = struct.pack("!I", len(payload_bytes))
    sock.sendall(header + payload_bytes)

def receive_packet(sock):
    header = sock.recv(4)
    if not header or len(header) < 4:
        return None
    length = struct.unpack("!I", header)[0]
    data_bytes = b""
    while len(data_bytes) < length:
        chunk = sock.recv(length - len(data_bytes))
        if not chunk:
            break
        data_bytes += chunk
    return data_bytes if len(data_bytes) == length else None

# --------------------- Local IP Detection ---------------------
def detect_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip_addr = s.getsockname()[0]
        s.close()
        return ip_addr
    except Exception:
        try:
            return socket.gethostbyname(socket.gethostname())
        except Exception:
            return "127.0.0.1"

# --------------------- Storage Functions ---------------------
def save_chunk(storage_dir, file_name, chunk_id, chunk_bytes):
    start_time = time.time()
    folder = Path(storage_dir) / file_name
    folder.mkdir(parents=True, exist_ok=True)
    local_path = folder / f"chunk_{chunk_id}"
    with open(local_path, "wb") as f:
        f.write(chunk_bytes)
    duration = time.time() - start_time
    print(f"[DNode-ID={DN_ID}] Stored chunk {chunk_id} ({format_bytes(len(chunk_bytes))}) for '{file_name}' in {duration:.4f}s at {local_path}")
    return local_path

def load_chunk(storage_dir, file_name, chunk_id):
    local_path = Path(storage_dir) / file_name / f"chunk_{chunk_id}"
    if not local_path.exists():
        print(f"[DNode-ID={DN_ID}] Requested chunk {chunk_id} of '{file_name}' not found")
        return None
    print(f"[DNode-ID={DN_ID}] Loaded chunk {chunk_id} of '{file_name}' for sending")
    return local_path.read_bytes()

# --------------------- Heartbeat Thread ---------------------
def heartbeat_loop():
    while True:
        try:
            hb_payload = {
                "id": DN_ID,
                "host": DN_HOST,
                "port": DN_PORT,
                "storage": STORAGE_DIR,
                "timestamp": time.time()
            }
            with socket.create_connection((NN_HOST, NN_PORT), timeout=5) as hb_sock:
                send_packet(hb_sock, json.dumps(hb_payload).encode())
                _ = receive_packet(hb_sock)

            folder_size = get_folder_size(STORAGE_DIR)
            print(f"[DNode-ID={DN_ID}] Heartbeat sent | Storage used: {format_bytes(folder_size)}")

        except Exception:
            print(f"[DNode-ID={DN_ID}] Heartbeat failed, retrying...")

        time.sleep(HB_INTERVAL)

# --------------------- Client Request Handler ---------------------
def process_client_request(sock, address):
    try:
        data = receive_packet(sock)
        if not data:
            return

        request = json.loads(data.decode("utf-8", errors="ignore"))
        cmd = request.get("cmd")

        print(f"[DNode-ID={DN_ID}] Received command '{cmd}' from {address}")

        # ---------- Store Chunk ----------
        if cmd == "store_chunk":
            chunk_info = request["meta"]
            chunk_data = receive_packet(sock)

            if not chunk_data:
                print(f"[DNode-ID={DN_ID}] No chunk bytes received")
                send_packet(sock, json.dumps({"status":"error","reason":"no_chunk"}).encode())
                return

            if compute_sha256(chunk_data) != chunk_info.get("sha256"):
                print(f"[DNode-ID={DN_ID}] Checksum mismatch for chunk {chunk_info['chunk_index']} of '{chunk_info['filename']}'")
                send_packet(sock, json.dumps({"status":"error","reason":"checksum_fail"}).encode())
                return

            save_chunk(STORAGE_DIR, chunk_info["filename"], chunk_info["chunk_index"], chunk_data)
            send_packet(sock, json.dumps({"status":"ok"}).encode())
            print(f"[DNode-ID={DN_ID}] Chunk stored successfully")
            return

        # ---------- Get Chunk ----------
        elif cmd == "get_chunk":
            chunk_info = request["meta"]
            chunk_data = load_chunk(STORAGE_DIR, chunk_info["filename"], chunk_info["chunk_index"])

            if not chunk_data:
                send_packet(sock, json.dumps({"status":"error","reason":"missing"}).encode())
                return

            send_packet(sock, json.dumps({"status":"ok","size": len(chunk_data)}).encode())
            send_packet(sock, chunk_data)
            print(f"[DNode-ID={DN_ID}] Served chunk {chunk_info['chunk_index']} of '{chunk_info['filename']}'")
            return

        # ---------- Replicate Chunk ----------
        elif cmd == "send_chunk_to":
            chunk_info = request["meta"]
            file_name = chunk_info["filename"]
            chunk_id = chunk_info["chunk_index"]
            dest_host = chunk_info["dest_host"]
            dest_port = int(chunk_info["dest_port"])

            print(f"[DNode-ID={DN_ID}] Replication request: chunk {chunk_id} -> {dest_host}:{dest_port}")

            chunk_bytes = load_chunk(STORAGE_DIR, file_name, chunk_id)
            if not chunk_bytes:
                send_packet(sock, json.dumps({"status":"error","reason":"local_missing"}).encode())
                print(f"[DNode-ID={DN_ID}] Replication failed — chunk not found locally")
                return

            try:
                with socket.create_connection((dest_host, dest_port), timeout=8) as dest_sock:
                    payload = {
                        "cmd":"store_chunk",
                        "meta":{
                            "filename": file_name,
                            "chunk_index": chunk_id,
                            "sha256": compute_sha256(chunk_bytes),
                            "size": len(chunk_bytes)
                        }
                    }

                    send_packet(dest_sock, json.dumps(payload).encode())
                    send_packet(dest_sock, chunk_bytes)

                    resp = receive_packet(dest_sock)

                    if resp and json.loads(resp.decode()).get("status") == "ok":
                        send_packet(sock, json.dumps({"status":"ok"}).encode())
                        print(f"[DNode-ID={DN_ID}] Replication completed successfully")
                    else:
                        send_packet(sock, json.dumps({"status":"error","reason":"dest_rejected"}).encode())
                        print(f"[DNode-ID={DN_ID}] Replication rejected by destination")

            except Exception as e:
                send_packet(sock, json.dumps({"status":"error","reason":"conn_failed","detail":str(e)}).encode())
                print(f"[DNode-ID={DN_ID}] Replication connection error: {e}")
            return

        # ---------- Unknown Command ----------
        else:
            print(f"[DNode-ID={DN_ID}] Unknown command '{cmd}' received")
            send_packet(sock, json.dumps({"status":"error","reason":"unknown_cmd"}).encode())

    except Exception as e:
        print(f"[DNode-ID={DN_ID}] Error processing client request: {e}")

    finally:
        sock.close()

# --------------------- Server Thread ---------------------
def run_chunk_server():
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(("0.0.0.0", DN_PORT))
    server_sock.listen(8)

    print(f"[DNode-ID={DN_ID}] Chunk server started on port {DN_PORT}")

    while True:
        client_sock, client_addr = server_sock.accept()
        print(f"[DNode-ID={DN_ID}] Incoming connection from {client_addr}")
        threading.Thread(target=process_client_request, args=(client_sock, client_addr), daemon=True).start()

# --------------------- Main ---------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--storage", required=True)
    parser.add_argument("--host", default=None)
    parser.add_argument("--namenode-host", default="127.0.0.1")
    parser.add_argument("--namenode-hb-port", default=9000, type=int)
    parser.add_argument("--hb-interval", default=3, type=int)
    args = parser.parse_args()

    DN_ID = str(args.id)
    DN_PORT = args.port
    STORAGE_DIR = args.storage
    DN_HOST = args.host or detect_local_ip()

    NN_HOST = args.namenode_host
    NN_PORT = args.namenode_hb_port
    HB_INTERVAL = args.hb_interval

    os.makedirs(STORAGE_DIR, exist_ok=True)

    print()
    print(f"[DNode-ID={DN_ID}] Datanode starting up")
    print(f" Host: {DN_HOST}:{DN_PORT}")
    print(f" Storage directory: {STORAGE_DIR}")
    print(f" Namenode: {NN_HOST}:{NN_PORT}")
    print()

    threading.Thread(target=run_chunk_server, daemon=True).start()
    threading.Thread(target=heartbeat_loop, daemon=True).start()

    while True:
        time.sleep(1)