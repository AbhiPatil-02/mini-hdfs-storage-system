#  Mini HDFS Simulation — Distributed File Storage System

##  Overview

The Mini HDFS Simulation project aims to replicate the fundamental architecture and behavior of the Hadoop Distributed File System (HDFS) using Python.  
This simulation demonstrates how data can be split into chunks, distributed across multiple DataNodes, and coordinated by a NameNode with fault tolerance and replication.

It is part of the PES University Big Data 2025 Project 1:  
**“Mini HDFS Simulation — Exploring Distributed Data Storage.”**

##  System Architecture

## Components

### NameNode (Node 2) — Central Metadata Manager and Controller
Tracks files and chunk-to-DataNode mappings, receives heartbeats, accepts DataNode notifications when chunks are stored, and triggers re-replication when needed.

---

### DataNodes

#### DataNode 0 (Node 3 — Storage Node A)
Receives and stores chunks, sends regular heartbeats to the NameNode, serves chunks to clients, and performs integrity checks (SHA-256 checksums).  
It also exposes endpoints to list, verify, and delete stored chunks.

**Key Responsibilities:**
- Chunk reception & storage  
- Heartbeat sender & connectivity  
- Retrieval logic for client downloads  
- Data integrity verification on retrieval  
- Logging and error handling  

---

#### DataNode 1 (Node 4 — Storage Node B, Secondary)
Acts as the replication / redundancy node. It accepts replica writes, verifies checksums, serves replica chunks to clients, sends heartbeats, runs periodic corruption detection, and participates in re-replication workflows.

**Key Responsibilities:**
- Accepting and verifying replica chunks  
- Heartbeat sender & connectivity  
- Serving replica chunks for client downloads and re-replication  
- Data validation and checksum consistency  
- Robustness & recovery during node failure tests  

---

### Client UI (Node 1) — Dashboard + Client REST Interface
Uploads files to the NameNode, downloads files by consulting NameNode metadata and fetching chunks from DataNodes, and shows a cluster view (metadata and DataNode status).

---

### Dashboard (Flask App)
Visualizes metadata, files, and DataNode status.


## Features

- **File upload/download via Client & Dashboard**
- **Files are split into fixed-size chunks** (default: 2 MB)
- **Configurable replication factor** (default: 2)
- **NameNode-driven re-replication** when replicas go missing
- **DataNodes compute and persist SHA-256 checksums** for integrity checks
- **REST APIs** for communication between NameNode ↔ DataNode ↔ Client
- **Persistent metadata** stored in `metadata.json`, with a background re-replication worker
- **Dashboard** provides a visual cluster view and simple file actions


##  Folder Structure


```plaintext
163_Project1_BD
├── client
│   ├── client.py
│   ├── config_client.json
│   └── templates
│       └── index.html
├── datanode0
│   ├── config_primary.json
│   └── datanode0.py
├── datanode1
│   ├── config_secondary.json
│   └── datanode1.py
├── namenode
│   ├── config_namenode.json
│   ├── namenode1.py
│   └── namenode.py
└── README.md
```


##  Team & Role Distribution

| Member       | Node                                      | Role                         | Responsibilities                                                                                                                                                                           |
| ------------ | ----------------------------------------- | ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Member 1** | **Node 1 – Client UI (Frontend)**         | Client Developer             | Develops the client interface and dashboard. Handles file upload/download via REST APIs, communicates with the NameNode, visualizes chunk distribution, and ensures smooth UI interaction. |
| **Member 2** | **Node 2 – NameNode (Controller)**        | NameNode Developer           | Designs and maintains metadata management. Implements file chunking, replication coordination, heartbeat reception, and re-replication logic for fault tolerance.                          |
| **Member 3** | **Node 3 – DataNode #0 (Storage Node A)** | Primary DataNode Developer   | Implements chunk reception and storage, heartbeat transmission to the NameNode, client chunk retrieval endpoints, and integrity validation to prevent corruption.                          |
| **Member 4** | **Node 4 – DataNode #1 (Storage Node B)** | Secondary DataNode Developer | Focuses on replication and redundancy. Handles replica verification, checksum consistency, recovery during node failures, and serving replica chunks to clients.                           |


##  System Requirements

- **Operating System**: Ubuntu 22.04+ (recommended for all nodes)
- **Python**: 3.10 or higher

### Libraries:
```bash
pip install flask requests pytest
```
### Hardware Requirements:
- Minimum 4 GB RAM per node
- 2 CPU cores
- Network connectivity between NameNode and all DataNodes


##  Configuration
**Default Parameters:**

| Parameter            | Default Value | Description                                     |
| -------------------- | ------------- | ----------------------------------------------- |
| `replication_factor` | 2             | Number of copies maintained for each file chunk |
| `namenode_host`      | localhost     | Host address of the NameNode                    |
| `namenode_port`      | 5000          | Port where the NameNode runs                    |
| `datanode0_port`     | 7000          | Port where DataNode 0 (Primary) runs            |
| `datanode1_port`     | 7001          | Port where DataNode 1 (Secondary) runs          |
| `client_port`        | 5001          | Port where the Client UI runs                   |
| `chunk_size`         | 2 MB          | Default size for each file chunk                |


You can modify these in a configuration file (config.json) or environment variables.

##  Getting Started

### 1️ Clone the Repository
```bash
git clone https://github.com/Pshejale/mini-hdfs-simulation.git
cd mini-hdfs-simulation
```

### 2️ Set Up Python Environment
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r namenode/requirements.txt
```
###  Running the System
**Step 1: Start the NameNode**
```bash
cd 163_Project1_BD/namenode
python3 namenode.py
# listens on config_namenode.json port (default 5000)
```

**Step 2: Start the DataNodes**
Run each DataNode in a new terminal or on separate Ubuntu systems:
```bash
#datanode0
cd ../datanode0
python3 datanode0.py
# reads port 7000 from config_primary.json
```
```
#datanode1
cd ../datanode1
python3 datanode1.py
# reads port 7001 from config_secondary.json
```

**Step 3:Start Client UI (dashboard)**
```bash
cd ../client
python3 client.py
# open http://localhost:5001
```

Now open:  http://localhost:5001 in your browser.


##  API Endpoints (What’s Implemented)

###  NameNode  
_File: `namenode/namenode.py`_

#### **Endpoints**
- **POST** `/register_datanode` — Register or update DataNode information  
- **POST** `/heartbeat` — Receive DataNode heartbeat  
- **POST** `/datanode_stored` — DataNode notifies NameNode after storing a chunk  
- **POST** `/report_corruption` — DataNode reports bad or corrupted chunks  
- **POST** `/upload` — Client uploads a file; NameNode splits it into chunks and instructs DataNodes to store them  
- **GET** `/metadata` — Return current metadata snapshot (files + DataNodes)  
- **GET** `/list_files` — List files with per-file summary  
- **POST** `/delete_file` — Instruct DataNodes to delete chunks and remove metadata entry  
- **POST** `/replicate_chunk` — Copy a chunk from one DataNode to another (used by the re-replication worker)  

---

###  DataNode  
_Files: `datanode0/`, `datanode1/`_

#### **Endpoints**
- **POST** `/store_chunk` — Store a chunk (atomic temp save → checksum → rename → index → notify NameNode)  
- **GET** `/get_chunk?chunk_id=...` — Retrieve a chunk (used by client and re-replication)  
- **POST** `/replicate_here` — Accept a replica sent by another DataNode  
- **GET** `/list_chunks` — List locally stored chunks and their checksums  
- **GET** `/verify_chunk?chunk_id=...` — Verify local checksum against stored checksum  
- **POST** `/delete_chunk` — Delete a chunk from storage and update index  

#### **Background Tasks**
- Heartbeat loop (regular updates to NameNode)  
- Periodic integrity check (checksum validation)  
- Startup validation and cleanup  

---

###  Client  
_File: `client.py`_

#### **Endpoints**
- **GET** `/` — Dashboard (fetches metadata from NameNode `/metadata`)  
- **POST** `/upload` — Upload a file to the NameNode (`/upload`)  
- **POST** `/download` — Reconstruct and download file by fetching chunks from DataNodes listed in metadata  

****

##  Workflow Summary

1. **Client uploads a file** to the NameNode (`multipart/form-data`).
2. **NameNode splits** the file into 2 MB chunks and selects DataNodes for each chunk based on the replication factor.
3. **NameNode sends chunk bytes** to the selected DataNodes (the client code performs this via the NameNode upload procedure).
4. **DataNodes store chunks atomically**, compute SHA-256 checksums, persist the local index, and notify the NameNode using `/datanode_stored`.
5. **NameNode updates** `metadata.json` with the chunk → DataNode mapping and persists it.
6. For **downloads**, the client fetches metadata and requests each chunk from an available DataNode;  
   if one DataNode is unreachable, it retries with another replica.

---

##  Fault Tolerance & Recovery

- **Heartbeats:** DataNodes send periodic heartbeats. If the NameNode misses heartbeats, it marks the nodes as offline.  
- **Re-replication:** NameNode runs a background re-replication worker to detect under-replicated chunks and triggers `/replicate_chunk` to copy a chunk from a live source node to a new target node.  
- **Corruption Reporting:** DataNodes perform periodic corruption checks and report bad chunks using `POST /report_corruption`.  
- **Startup Validation:** On startup, DataNodes remove stale `.tmp` files and reconcile index entries with actual disk contents.  



###  References

- [Hadoop Distributed File System Architecture Guide (Apache)](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html)
- [PESU Big Data 2025 Project Guidelines](https://pesubigdata2025.super.site/projects/project-1-mini-hdfs-simulation-exploring-distributed-data-storage)
- [Flask Documentation](https://flask.palletsprojects.com)


