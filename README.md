# Mini-HDFS-System
Mini HDFS Simulation-Exploring Distributed Data Storage

##  Overview
This project simulates a **simplified version of the Hadoop Distributed File System (HDFS)** — the core of distributed data storage systems.  
The goal is to understand how files are divided into chunks, replicated across multiple datanodes, and managed by a central namenode for reliability and scalability.

This simulation demonstrates:
- Distributed storage of files across datanodes
- Heartbeat monitoring for node health
- Automatic re-replication on datanode failure
- File upload/download using a simple web client

##  **System Components**

### 1. Namenode

The **Namenode** acts as the central controller of the distributed file system.  
It manages **metadata**, tracks **chunk-to-datanode mappings**, monitors **heartbeat signals**, and initiates **re-replication** in case of datanode failure.

#### Key Responsibilities
- Maintains a directory of all files, chunks, and their corresponding datanodes.  
- Receives heartbeats from datanodes to monitor their health.  
- Detects dead datanodes and automatically triggers re-replication of affected chunks.  
- Provides metadata and datanode assignment info to clients during file upload/download.  
- Exposes REST APIs (via Flask) for the client and datanodes to interact with.  

#### Major Features
- **Heartbeat Listener (Port 9000):**  
  Listens for periodic heartbeats from each datanode.  
- **Flask API Server (Port 8000):**  
  Handles requests for:
  - `/register` – Register a new datanode  
  - `/heartbeat` – Receive datanode heartbeat updates  
  - `/allocate` – Assign datanodes for file chunks  
  - `/datanodes` – Display current datanode status  

#### Re-replication Handling
When a datanode fails (heartbeat missing beyond threshold), the Namenode identifies all chunks stored on that node and attempts to re-replicate them to healthy datanodes to maintain the replication factor.

#### Run Cmd:
    python3 namenode.py

### 2. Datanode #0 (Storage Node A)

Datanode-0 receives file chunks, stores them safely, and sends them back when needed.  
It also sends periodic heartbeats to the Namenode to report its status.

Supports:

- Receiving incoming file chunks and storing them on disk.
- Sending periodic heartbeats to the Namenode to report health and status.
- Serving stored chunks back to the client during downloads.
- Verifying chunk integrity using checksum to prevent corruption.
- Logging all operations with basic error handling.

#### Run Cmd:
    python3 datanode.py --id dn1 --port 9001 --storage ./dn1_store --host 172.25.169.217 --namenode-host 172.25.5.104

 ### 3. Datanode #1 (Storage Node B)

Datanode-1 extends the distributed storage system by managing an additional data node that handles chunk storage, retrieval, and replication for fault tolerance.
It maintains synchronization with the Namenode and ensures data integrity during transfers.

Supports:

- Receiving and storing file chunks sent by the Namenode or other Datanodes.
- Performing SHA-256 checksum verification to ensure chunk integrity.
- Sending regular heartbeats to the Namenode to confirm active status and storage capacity.
- Responding to client or peer Datanode requests for chunk retrieval and replication.
- Logging key events for monitoring and debugging to maintain reliable operation.

#### Run Cmd:
    python3 datanode.py --id dn2 --port 9002 --storage ./dn2_store --host 172.25.124.103 --namenode-host 172.25.5.104

###  4. Client UI

Flask-based web interface for users.

Supports:

- Uploading files (split into chunks & replicated).
- Downloading files (reconstructed from chunks).
- Viewing connected Datanodes and their live status.
- Checking chunk-to-datanode mapping in real time.

#### Run Cmd:
    python3 client_ui.py --namenode-api http://172.25.5.104:8000 --port 7000
