#!/usr/bin/env python3
import os
import json
import threading
import time
from flask import Flask, request, jsonify
import requests

# === PATH SETUP ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  
META_PATH = os.path.join(BASE_DIR, "metadata.json")
CONFIG_PATH = os.path.join(BASE_DIR, "config_namenode.json")

# === LOAD CONFIG ===
with open(CONFIG_PATH, "r") as f:
    CONFIG = json.load(f)

app = Flask(__name__) 
lock = threading.RLock()  


if os.path.exists(META_PATH):
    with open(META_PATH, "r") as f:
        metadata = json.load(f)
else:
    metadata = {"files": {}, "datanodes": {}}


def persist_metadata():
    """Persist metadata safely - assumes lock is already held."""
    try:
        with open(META_PATH, "w") as f:
            json.dump(metadata, f, indent=2)
    except Exception as e:
        app.logger.error(f"Error persisting metadata: {e}")


def get_alive_datanodes(timeout=15):
    """Return datanodes that sent heartbeat within timeout seconds."""
    now = time.time()
    alive = {}
    with lock:
        for node_id, info in metadata["datanodes"].items():
            if now - info.get("last_heartbeat", 0) < timeout:
                alive[node_id] = info
    return alive


def pick_targets_for_chunk(replication, chunk_index):
    """Round-robin choose datanodes for a chunk."""
    alive = list(get_alive_datanodes().keys())
    
    # FIXED #8: Handle empty alive list
    if not alive:
        return []
    
    targets = []
    for r in range(replication):
        idx = (chunk_index + r) % len(alive)
        targets.append(alive[idx])
    return targets


# === ROUTES ===
@app.route("/register_datanode", methods=["POST"])
def register_datanode():
    """Register a new datanode or update its record."""
    try:
        body = request.get_json()
        
        # FIXED #6: Add validation
        if not body:
            return jsonify({"status": "error", "msg": "empty request body"}), 400
        
        node_id = body.get("node_id")
        ip = body.get("ip")
        port = body.get("port", CONFIG.get("datanode_port", 7000))
        
        if not node_id or not ip:
            return jsonify({"status": "error", "msg": "missing node_id or ip"}), 400
        
        with lock:
            metadata["datanodes"][node_id] = {
                "ip": ip,
                "port": port,
                "last_heartbeat": time.time(),
                "role": body.get("role", "datanode"),
                "chunk_count": body.get("stored_chunks", 0)
            }
            persist_metadata()
        
        app.logger.info(f"Datanode registered: {node_id} ({ip}:{port})")
        return jsonify({"status": "ok"})
    
    except Exception as e:
        app.logger.exception(f"Error in register_datanode: {e}")
        return jsonify({"status": "error", "msg": str(e)}), 500


@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    """Receive periodic heartbeat from datanodes."""
    try:
        body = request.get_json() or {}
        node_id = body.get("node_id")
        
        if not node_id:
            return jsonify({"status": "error", "msg": "missing node_id"}), 400
        
        now = time.time()
        with lock:
            if node_id in metadata["datanodes"]:
                metadata["datanodes"][node_id]["last_heartbeat"] = now
                if "stored_chunks" in body:
                    metadata["datanodes"][node_id]["chunk_count"] = body.get("stored_chunks", 0)
            else:
                # Auto-register if unknown node sends heartbeat
                metadata["datanodes"][node_id] = {
                    "ip": body.get("ip", "unknown"),
                    "port": body.get("port", CONFIG.get("datanode_port", 7000)),
                    "last_heartbeat": now,
                    "role": body.get("role", "datanode"),
                    "chunk_count": body.get("stored_chunks", 0)
                }
            persist_metadata()
        
        app.logger.debug(f"Heartbeat received from {node_id}")
        return jsonify({"status": "ok"})
    
    except Exception as e:
        app.logger.exception(f"Error in heartbeat: {e}")
        return jsonify({"status": "error", "msg": str(e)}), 500


@app.route("/datanode_stored", methods=["POST"])
def datanode_stored():
    """
    Datanodes call this endpoint after they store a chunk.
    Update last_heartbeat and record chunk replica.
    """
    try:
        body = request.get_json() or {}
        node_id = body.get("node_id")
        chunk_id = body.get("chunk_id")
        checksum = body.get("checksum")
        
        if not node_id or not chunk_id:
            return jsonify({"status": "error", "msg": "missing node_id or chunk_id"}), 400
        
        now = time.time()
        with lock:
            # Update datanode heartbeat
            if node_id in metadata["datanodes"]:
                metadata["datanodes"][node_id]["last_heartbeat"] = now
            else:
                metadata["datanodes"][node_id] = {
                    "ip": body.get("ip", "unknown"),
                    "port": body.get("port", CONFIG.get("datanode_port", 7000)),
                    "last_heartbeat": now
                }
            
            # FIXED #4 & #5: Record chunk replica properly
            for fname, fmeta in metadata.get("files", {}).items():
                if chunk_id in fmeta.get("chunks", {}):
                    # chunks[chunk_id] is a list of node_ids
                    if node_id not in fmeta["chunks"][chunk_id]:
                        fmeta["chunks"][chunk_id].append(node_id)
            
            persist_metadata()
        
        app.logger.info(f"Received datanode_stored from {node_id} for {chunk_id}")
        return jsonify({"status": "ok"})
    
    except Exception as e:
        app.logger.exception(f"Error in datanode_stored: {e}")
        return jsonify({"status": "error", "msg": str(e)}), 500


@app.route("/report_corruption", methods=["POST"])
def report_corruption():
    """Handle corruption report from datanode."""
    try:
        body = request.get_json() or {}
        node_id = body.get("node_id")
        bad_chunks = body.get("bad_chunks", [])
        
        if not node_id:
            return jsonify({"status": "error", "msg": "missing node_id"}), 400
        
        app.logger.warning(f"Corruption report from {node_id}: {bad_chunks}")
        
        # Remove corrupted chunks from replica list
        with lock:
            for fname, fmeta in metadata.get("files", {}).items():
                for chunk_id in bad_chunks:
                    if chunk_id in fmeta.get("chunks", {}):
                        fmeta["chunks"][chunk_id] = [n for n in fmeta["chunks"][chunk_id] if n != node_id]
            persist_metadata()
        
        return jsonify({"status": "ok"})
    
    except Exception as e:
        app.logger.exception(f"Error in report_corruption: {e}")
        return jsonify({"status": "error", "msg": str(e)}), 500


@app.route("/upload", methods=["POST"])
def upload_file():
    """
    Client uploads a file via multipart/form-data.
    Namenode chunks the file and instructs datanodes to store chunks.
    """
    try:
        replication = CONFIG.get("replication_factor", 2)
        chunk_size = CONFIG.get("chunk_size", 2 * 1024 * 1024)  # 2 MB default

        f = request.files.get("file")
        if not f:
            return jsonify({"status": "error", "msg": "No file provided"}), 400
        
        filename = request.form.get("filename") or f.filename
        data = f.read()
        total = len(data)
        num_chunks = (total + chunk_size - 1) // chunk_size
        
        app.logger.info(f"Uploading {filename}, size={total} bytes → {num_chunks} chunks")

        file_entry = {"chunks": {}, "num_chunks": num_chunks, "size": total}
        alive_nodes = get_alive_datanodes()
        
        if len(alive_nodes) < replication:
            return jsonify({
                "status": "error",
                "reason": "Not enough datanodes alive",
                "needed": replication,
                "alive": list(alive_nodes.keys())
            }), 400

        for i in range(num_chunks):
            start = i * chunk_size
            part = data[start:start + chunk_size]
            chunk_id = f"{filename}.chunk{i}"
            targets = pick_targets_for_chunk(replication, i)
            stored_on = []

            for node_id in targets:
                dn = metadata["datanodes"].get(node_id)
                if not dn:
                    continue
                
                url = f"http://{dn['ip']}:{dn['port']}/store_chunk"
                try:
                    files = {"chunk": (chunk_id, part)}
                    
                    # Robust retry with longer timeout and small backoff
                    success = False
                    backoff = 1
                    
                    for attempt in range(1, 4):  # up to 3 attempts
                        try:
                            resp = requests.post(
                                url,
                                files=files,
                                data={"chunk_id": chunk_id, "file_name": filename, "chunk_size": len(part)},
                                timeout=60,
                            )
                            
                            if resp.status_code == 200:
                                stored_on.append(node_id)
                                app.logger.info(f"Stored {chunk_id} on {node_id}")
                                success = True
                                break
                            else:
                                app.logger.warning(f"Failed to store {chunk_id} on {node_id}: "
                                                 f"{resp.status_code} {resp.text[:200]}")
                        
                        except requests.exceptions.Timeout:
                            app.logger.warning(f"Timeout storing {chunk_id} on {node_id}, attempt {attempt}")
                        except Exception as e:
                            app.logger.warning(f"Error storing {chunk_id} on {node_id}: {e}")
                        
                        if attempt < 3:
                            time.sleep(backoff)
                            backoff = min(backoff * 2, 8)
                    
                    if not success:
                        app.logger.warning(f"Giving up storing {chunk_id} on {node_id} after retries")
                
                except Exception as e:
                    app.logger.warning(f"Exception storing {chunk_id} on {node_id}: {e}")

            if not stored_on:
                return jsonify({
                    "status": "error",
                    "msg": f"Chunk {chunk_id} failed to store on any datanode"
                }), 500

            file_entry["chunks"][chunk_id] = stored_on

        with lock:
            metadata["files"][filename] = file_entry
            persist_metadata()

        app.logger.info(f"File upload complete: {filename} ({num_chunks} chunks)")
        return jsonify({"status": "ok", "filename": filename, "num_chunks": num_chunks})
    
    except Exception as e:
        app.logger.exception(f"Error in upload_file: {e}")
        return jsonify({"status": "error", "msg": str(e)}), 500


@app.route("/metadata", methods=["GET"])
def get_metadata():
    """Get current metadata state."""
    with lock:
        return jsonify(metadata)


# FIXED #9: Add list_files endpoint
@app.route("/list_files", methods=["GET"])
def list_files():
    """List all stored files and their metadata."""
    try:
        with lock:
            files_list = []
            for fname, fmeta in metadata.get("files", {}).items():
                files_list.append({
                    "filename": fname,
                    "size": fmeta.get("size", 0),
                    "num_chunks": fmeta.get("num_chunks", 0),
                    "chunks": {
                        cid: len(nodes) for cid, nodes in fmeta.get("chunks", {}).items()
                    }
                })
        
        return jsonify({"files": files_list, "total": len(files_list)})
    
    except Exception as e:
        app.logger.exception(f"Error in list_files: {e}")
        return jsonify({"status": "error", "msg": str(e)}), 500


@app.route("/replicate_chunk", methods=["POST"])
def replicate_chunk():
    """Copy a chunk from one datanode to another (re-replication)."""
    try:
        body = request.get_json()
        chunk_id = body.get("chunk_id")
        src = body.get("src_node")
        dest = body.get("dest_node")
        
        if not all([chunk_id, src, dest]):
            return jsonify({"status": "error", "msg": "missing parameters"}), 400
        
        with lock:
            src_dn = metadata["datanodes"].get(src)
            dest_dn = metadata["datanodes"].get(dest)
        
        if not src_dn or not dest_dn:
            return jsonify({"status": "error", "msg": "Unknown datanode"}), 400
        
        try:
            # Fetch chunk from source
            r = requests.get(f"http://{src_dn['ip']}:{src_dn['port']}/get_chunk",
                           params={"chunk_id": chunk_id}, timeout=60)
            if r.status_code != 200:
                return jsonify({"status": "error", "msg": "Fetch failed"}), 400
            
            content = r.content
            
            # Store on destination
            files = {"chunk": (chunk_id, content)}
            resp = requests.post(f"http://{dest_dn['ip']}:{dest_dn['port']}/store_chunk",
                               files=files, data={"chunk_id": chunk_id}, timeout=60)
            
            if resp.status_code == 200:
                # Update metadata
                with lock:
                    for fname, fmeta in metadata.get("files", {}).items():
                        if chunk_id in fmeta.get("chunks", {}):
                            if dest not in fmeta["chunks"][chunk_id]:
                                fmeta["chunks"][chunk_id].append(dest)
                            persist_metadata()
                            break
                
                app.logger.info(f"Re-replicated {chunk_id} from {src} to {dest}")
                return jsonify({"status": "ok"})
            else:
                return jsonify({"status": "error", "msg": "Destination store failed"}), 500
        
        except Exception as e:
            app.logger.exception(f"Replication error: {e}")
            return jsonify({"status": "error", "msg": str(e)}), 500
    
    except Exception as e:
        app.logger.exception(f"Error in replicate_chunk: {e}")
        return jsonify({"status": "error", "msg": str(e)}), 500


# FIXED #10: Better lock handling in re_replication_worker
def re_replication_worker():
    """Background thread to detect under-replicated chunks and fix them."""
    while True:
        time.sleep(10)
        
        try:
            with lock:
                files = dict(metadata.get("files", {}))
                dnodes = dict(metadata.get("datanodes", {}))
            
            alive_nodes = [nid for nid, info in dnodes.items()
                          if time.time() - info.get("last_heartbeat", 0) < 15]

            for fname, fmeta in files.items():
                for chunk_id, hosts in list(fmeta.get("chunks", {}).items()):
                    alive_hosts = [h for h in hosts if h in alive_nodes]
                    replication_factor = CONFIG.get("replication_factor", 2)
                    
                    if len(alive_hosts) < replication_factor:
                        if not alive_hosts:
                            app.logger.warning(f"{chunk_id} has no alive replicas!")
                            continue
                        
                        src = alive_hosts[0]
                        candidates = [n for n in alive_nodes if n not in hosts]
                        
                        if not candidates:
                            app.logger.warning(f"No candidate datanodes for re-replicating {chunk_id}")
                            continue
                        
                        dest = candidates[0]
                        
                        try:
                            app.logger.info(f"Re-replicating {chunk_id} from {src} to {dest}")
                            requests.post(f"http://{CONFIG['ip']}:{CONFIG['namenode_port']}/replicate_chunk",
                                        json={"chunk_id": chunk_id, "src_node": src, "dest_node": dest},
                                        timeout=120)
                        except Exception as e:
                            app.logger.warning(f"Re-replication error: {e}")
        
        except Exception as e:
            app.logger.exception(f"Error in re_replication_worker: {e}")


# FIXED #7: if __name__ == "__main__":
if __name__ == "__main__":
    app.logger.info("=" * 70)
    app.logger.info("Starting NameNode")
    app.logger.info("=" * 70)
    app.logger.info(f"NameNode listening on {CONFIG['ip']}:{CONFIG['namenode_port']}")
    
    t = threading.Thread(target=re_replication_worker, daemon=True)
    t.start()
    
    app.run(host=CONFIG["ip"], port=CONFIG["namenode_port"], debug=CONFIG.get("debug", False), threaded=True)
