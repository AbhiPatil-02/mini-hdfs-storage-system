#!/usr/bin/env python3
"""
Datanode Primary (Node 3) - Storage Node A
Fully compatible with corrected NameNode
"""

import os, json, time, threading, hashlib, logging, re
from time import perf_counter
from flask import Flask, request, jsonify, send_file
import requests

# ============================================================
# Setup & Configuration
# ============================================================
BASE = os.path.dirname(os.path.abspath(__file__))
STORAGE = os.path.join(BASE, "storage")
INDEX = os.path.join(BASE, "chunks_index_primary.json")
CONFIG_PATH = os.path.join(BASE, "config_primary.json")
os.makedirs(STORAGE, exist_ok=True)

# Default config
DEFAULT_CONFIG = {
    "node_id": "DataNode_0",
    "ip": "0.0.0.0",
    "port": 7000,
    "namenode_ip": "172.22.78.202", 
    "namenode_port": 5000,
    "heartbeat_interval": 5,
    "debug": True,
    "secondaries": []
}

# Load config
try:
    with open(CONFIG_PATH) as f:
        loaded_config = json.load(f)
        C = {**DEFAULT_CONFIG, **loaded_config}
except Exception as e:
    print(f"Warning: Using default config ({e})")
    C = DEFAULT_CONFIG

NODE_ID = C["node_id"]
MY_IP = C.get("ip", "0.0.0.0")
MY_PORT = C["port"]
NAMENODE_IP = C["namenode_ip"]
NAMENODE_PORT = C["namenode_port"]
HEARTBEAT_INTERVAL = C["heartbeat_interval"]
REPLICA_PUSH = C.get("push_replica_on_store", True)
SECONDARIES = [s for s in C.get("secondaries", []) if "ip" in s and "node_id" in s]

FLASK_HOST = "0.0.0.0"  # Listen on all interfaces
FLASK_PORT = MY_PORT

# ============================================================
# Logging setup
# ============================================================
log_level = logging.DEBUG if C.get("debug", False) else logging.INFO
logging.basicConfig(
    level=log_level,
    format=f'%(asctime)s [{NODE_ID}] %(levelname)s: %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(BASE, "datanode_primary.log"), mode='a')
    ]
)
logger = logging.getLogger(__name__)

# ============================================================
# Flask App
# ============================================================
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024

# ============================================================
# Index handling
# ============================================================
if os.path.exists(INDEX):
    with open(INDEX) as f:
        index = json.load(f)
else:
    index = {}

index_lock = threading.RLock()

def persist_index():
    """Persist chunk index safely."""
    tmp = INDEX + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(index, f, indent=2)
        os.replace(tmp, INDEX)
    except Exception as e:
        logger.error(f"Error persisting index: {e}")

def validate_chunk_id(chunk_id):
    """Validate chunk_id to prevent path traversal."""
    if not chunk_id:
        return False
    return bool(re.match(r'^[a-zA-Z0-9._-]+$', chunk_id))

def validate_path_in_storage(path):
    """Verify path is within STORAGE directory."""
    try:
        real_path = os.path.realpath(path)
        real_storage = os.path.realpath(STORAGE)
        return real_path.startswith(real_storage)
    except:
        return False

def cleanup_orphaned_chunks():
    """Remove chunks from disk that aren't in index."""
    logger.info("Cleaning up orphaned chunks...")
    try:
        files_on_disk = set(os.listdir(STORAGE))
        indexed_files = set(os.path.basename(rec["path"]) for rec in index.values() if "path" in rec)
        
        orphaned = files_on_disk - indexed_files
        for orphan in orphaned:
            try:
                orphan_path = os.path.join(STORAGE, orphan)
                os.remove(orphan_path)
                logger.info(f"Removed orphaned chunk: {orphan}")
            except Exception as e:
                logger.error(f"Failed to remove orphan {orphan}: {e}")
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")

# ============================================================
# Utilities
# ============================================================
def sha256_stream(path):
    """Compute sha256 without loading entire file into memory."""
    h = hashlib.sha256()
    try:
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception as e:
        logger.error(f"Error computing checksum for {path}: {e}")
        return None

def async_notify_and_replicate(chunk_id, path, checksum):
    """Background worker: notify namenode and push replicas."""
    # Notify Namenode
    try:
        logger.debug(f"BG: notifying namenode about {chunk_id}")
        requests.post(
            f"http://{NAMENODE_IP}:{NAMENODE_PORT}/datanode_stored",
            json={"node_id": NODE_ID, "chunk_id": chunk_id, "checksum": checksum},
            timeout=5,
        )
        logger.info(f"BG: Namenode notified about {chunk_id}")
    except Exception as e:
        logger.warning(f"BG: notify namenode failed for {chunk_id}: {e}")

    # Push replicas to secondaries
    if not SECONDARIES:
        return
    
    for s in SECONDARIES:
        if "ip" not in s or "node_id" not in s:
            logger.warning(f"Skipping malformed secondary: {s}")
            continue
            
        try:
            with open(path, "rb") as fh:
                chunk_data = fh.read()
            
            files = {"chunk": (chunk_id, chunk_data)}
            data = {"chunk_id": chunk_id, "origin": NODE_ID, "is_replica": "true"}
            
            resp = requests.post(
                f"http://{s['ip']}:{s.get('port', 7001)}/replicate_here",
                files=files, data=data, timeout=10,
            )
            
            if resp.status_code == 200:
                logger.info(f"BG: pushed replica {chunk_id} -> {s['node_id']}")
            else:
                logger.warning(f"BG: replica push {chunk_id} -> {s['node_id']} failed: {resp.status_code}")
        except Exception as e:
            logger.warning(f"BG: exception pushing replica to {s.get('node_id')}: {e}")

# ============================================================
# Routes
# ============================================================
@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint for NameNode."""
    try:
        return jsonify({
            "status": "healthy",
            "node_id": NODE_ID,
            "port": MY_PORT,
            "stored_chunks": len(index),
            "uptime": time.time()
        }), 200
    except Exception as e:
        logger.error(f"Health check error: {e}")
        return jsonify({"status": "error", "msg": str(e)}), 500

@app.route("/store_chunk", methods=["POST"])
def store_chunk():
    """Store chunk safely with validation."""
    chunk_file = request.files.get("chunk")
    chunk_id = request.form.get("chunk_id") or (chunk_file.filename if chunk_file else None)
    origin = request.form.get("origin", "client")
    
    declared_size = request.form.get("chunk_size")
    if declared_size:
        try:
            declared_size = int(declared_size)
        except ValueError:
            return jsonify({"status": "error", "msg": "invalid chunk_size"}), 400

    if not chunk_file or not chunk_id:
        return jsonify({"status": "error", "msg": "missing chunk or chunk_id"}), 400

    if not validate_chunk_id(chunk_id):
        return jsonify({"status": "error", "msg": "invalid chunk_id format"}), 400

    t0 = perf_counter()
    temp_path = None
    
    try:
        temp_path = os.path.join(STORAGE, f"{chunk_id}.tmp")
        final_path = os.path.join(STORAGE, chunk_id)
        
        logger.debug(f"store_chunk: saving {chunk_id}")

        save_start = perf_counter()
        chunk_file.save(temp_path)
        save_end = perf_counter()
        
        actual_size = os.path.getsize(temp_path)
        
        if declared_size and actual_size != declared_size:
            os.remove(temp_path)
            logger.warning(f"Size mismatch for {chunk_id}: declared={declared_size}, actual={actual_size}")
            return jsonify({
                "status": "error",
                "msg": f"size mismatch: expected {declared_size}, got {actual_size}"
            }), 400

        checksum = sha256_stream(temp_path)
        if not checksum:
            os.remove(temp_path)
            return jsonify({"status": "error", "msg": "checksum computation failed"}), 500
        
        cs_end = perf_counter()

        with index_lock:
            if os.path.exists(final_path):
                os.remove(final_path)
            
            os.rename(temp_path, final_path)
            temp_path = None
            
            index[chunk_id] = {
                "path": final_path,
                "checksum": checksum,
                "origin": origin,
                "stored_at": time.time(),
                "size": actual_size
            }
            persist_index()

        logger.info(f"Stored chunk {chunk_id} checksum={checksum} size={actual_size} "
                   f"(save={save_end - save_start:.3f}s, checksum={cs_end - save_end:.3f}s, "
                   f"total={perf_counter() - t0:.3f}s)")

        if REPLICA_PUSH:
            threading.Thread(target=async_notify_and_replicate,
                             args=(chunk_id, final_path, checksum),
                             daemon=True).start()

        return jsonify({"status": "ok", "chunk_id": chunk_id, "checksum": checksum, "size": actual_size}), 200
    
    except Exception as e:
        logger.exception(f"store_chunk exception: {e}")
        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except:
                pass
        return jsonify({"status": "error", "msg": str(e)}), 500

@app.route("/get_chunk", methods=["GET"])
def get_chunk():
    """Retrieve chunk with verification."""
    chunk_id = request.args.get("chunk_id")
    if not chunk_id:
        return jsonify({"status": "error", "msg": "missing chunk_id"}), 400

    if not validate_chunk_id(chunk_id):
        return jsonify({"status": "error", "msg": "invalid chunk_id format"}), 400

    with index_lock:
        rec = index.get(chunk_id)
    
    if not rec:
        logger.warning(f"get_chunk not found in index: {chunk_id}")
        return jsonify({"status": "error", "msg": "not found"}), 404
    
    chunk_path = rec.get("path")
    
    if not chunk_path or not validate_path_in_storage(chunk_path):
        logger.error(f"Invalid path for chunk {chunk_id}: {chunk_path}")
        return jsonify({"status": "error", "msg": "invalid chunk path"}), 500
    
    if not os.path.exists(chunk_path):
        logger.error(f"Chunk file not found on disk: {chunk_id}")
        with index_lock:
            del index[chunk_id]
            persist_index()
        return jsonify({"status": "error", "msg": "file not found"}), 404

    current_checksum = sha256_stream(chunk_path)
    if current_checksum != rec.get("checksum"):
        logger.error(f"Checksum mismatch on retrieval: {chunk_id}")
        return jsonify({"status": "error", "msg": "data corruption detected"}), 500

    logger.debug(f"get_chunk: sending {chunk_id}")
    try:
        return send_file(chunk_path, as_attachment=True, download_name=chunk_id)
    except Exception as e:
        logger.error(f"Error sending chunk {chunk_id}: {e}")
        return jsonify({"status": "error", "msg": str(e)}), 500

@app.route("/list_chunks", methods=["GET"])
def list_chunks():
    """List all stored chunks."""
    with index_lock:
        chunks_list = [
            {
                "chunk_id": cid,
                "size": rec.get("size", 0),
                "checksum": rec.get("checksum"),
                "stored_at": rec.get("stored_at")
            }
            for cid, rec in index.items()
        ]
    return jsonify({"chunks": chunks_list, "total": len(chunks_list)}), 200

@app.route("/replicate_here", methods=["POST"])
def replicate_here():
    """Accept replica chunks from primary datanode."""
    chunk_file = request.files.get("chunk")
    chunk_id = request.form.get("chunk_id")
    origin = request.form.get("origin", "unknown")
    is_replica = request.form.get("is_replica", "false").lower() == "true"

    if not chunk_file or not chunk_id:
        return jsonify({"status": "error", "msg": "missing chunk or chunk_id"}), 400

    if not validate_chunk_id(chunk_id):
        return jsonify({"status": "error", "msg": "invalid chunk_id format"}), 400

    try:
        path = os.path.join(STORAGE, f"{chunk_id}_replica")
        logger.debug(f"Storing replica: {chunk_id} from {origin}")

        chunk_file.save(path)
        checksum = sha256_stream(path)

        with index_lock:
            index[f"{chunk_id}_replica"] = {
                "path": path,
                "checksum": checksum,
                "origin": origin,
                "is_replica": is_replica,
                "stored_at": time.time()
            }
            persist_index()

        logger.info(f"Replica stored: {chunk_id} (checksum={checksum})")
        return jsonify({"status": "ok", "chunk_id": chunk_id, "checksum": checksum}), 200
    
    except Exception as e:
        logger.exception(f"replicate_here exception: {e}")
        return jsonify({"status": "error", "msg": str(e)}), 500

@app.route("/verify_chunk", methods=["GET"])
def verify_chunk():
    """Verify chunk integrity."""
    chunk_id = request.args.get("chunk_id")
    if not chunk_id:
        return jsonify({"status": "error", "msg": "missing chunk_id"}), 400

    with index_lock:
        rec = index.get(chunk_id)
    
    if not rec:
        return jsonify({"status": "error", "msg": "not found"}), 404
    
    chunk_path = rec.get("path")
    if not os.path.exists(chunk_path):
        return jsonify({"status": "error", "msg": "file missing on disk"}), 500
    
    current_checksum = sha256_stream(chunk_path)
    stored_checksum = rec.get("checksum")
    
    if current_checksum == stored_checksum:
        return jsonify({
            "status": "ok",
            "chunk_id": chunk_id,
            "checksum": current_checksum,
            "verified": True
        }), 200
    else:
        return jsonify({
            "status": "error",
            "msg": "checksum mismatch",
            "expected": stored_checksum,
            "actual": current_checksum,
            "verified": False
        }), 500

# ============================================================
# Heartbeat Loop
# ============================================================
def heartbeat_loop():
    """Send periodic heartbeats to NameNode with exponential backoff."""
    url = f"http://{NAMENODE_IP}:{NAMENODE_PORT}/heartbeat"
    
    consecutive_failures = 0
    
    while True:
        try:
            payload = {
                "node_id": NODE_ID,
                "ip": MY_IP,
                "port": FLASK_PORT,
                "role": "datanode_primary",
                "stored_chunks": len(index),
                "timestamp": time.time()
            }
            
            response = requests.post(url, json=payload, timeout=5)
            if response.status_code == 200:
                logger.debug("Heartbeat sent successfully")
                consecutive_failures = 0
            else:
                logger.warning(f"Heartbeat returned status {response.status_code}")
                consecutive_failures += 1
        except requests.exceptions.Timeout:
            logger.warning(f"Heartbeat timeout to NameNode")
            consecutive_failures += 1
        except requests.exceptions.ConnectionError:
            logger.warning(f"⚠ Failed to connect to NameNode at {NAMENODE_IP}:{NAMENODE_PORT}")
            consecutive_failures += 1
        except Exception as e:
            logger.warning(f"⚠ Heartbeat error: {str(e)}")
            consecutive_failures += 1
        
        # Calculate sleep time
        if consecutive_failures == 0:
            sleep_time = HEARTBEAT_INTERVAL
        else:
            sleep_time = min(HEARTBEAT_INTERVAL * (2 ** (consecutive_failures - 1)), 30)
        
        logger.debug(f"Next heartbeat in {sleep_time}s (failures: {consecutive_failures})")
        time.sleep(sleep_time)

# ============================================================
# Validation on Startup
# ============================================================
def validate_on_startup():
    """Validate stored chunks on startup."""
    logger.info("Validating local stored chunks on startup...")
    bad = []
    
    with index_lock:
        for cid, rec in list(index.items()):
            try:
                chunk_path = rec.get("path")
                
                if not chunk_path or not os.path.exists(chunk_path):
                    bad.append(cid)
                    logger.error(f"Missing chunk on disk: {cid}")
                    continue
                
                current_checksum = sha256_stream(chunk_path)
                stored_checksum = rec.get("checksum")
                
                if current_checksum != stored_checksum:
                    bad.append(cid)
                    logger.error(f"Checksum mismatch on startup: {cid}")
            except Exception as e:
                logger.exception(f"Error validating {cid}: {e}")
    
    with index_lock:
        for bad_cid in bad:
            if bad_cid in index:
                try:
                    chunk_path = index[bad_cid].get("path")
                    if chunk_path and os.path.exists(chunk_path):
                        os.remove(chunk_path)
                except:
                    pass
                del index[bad_cid]
        
        if bad:
            persist_index()
    
    cleanup_orphaned_chunks()
    
    if bad:
        logger.warning(f"Found {len(bad)} bad chunks. Reporting to Namenode...")
        try:
            requests.post(f"http://{NAMENODE_IP}:{NAMENODE_PORT}/report_corruption",
                          json={"node_id": NODE_ID, "bad_chunks": bad}, timeout=5)
        except Exception:
            logger.debug("Could not report corruption to namenode")

# Live corruption checks
def periodic_corruption_check(interval=60):
    while True:
        bad = []
        with index_lock:
            for cid, rec in list(index.items()):
                chunk_path = rec.get("path")
                if not chunk_path or not os.path.exists(chunk_path):
                    bad.append(cid)
                else:
                    current_checksum = sha256_stream(chunk_path)
                    stored_checksum = rec.get("checksum")
                    if current_checksum != stored_checksum:
                        bad.append(cid)
        if bad:
            # Remove bad entries and files
            with index_lock:
                for bad_cid in bad:
                    if bad_cid in index:
                        chunk_path = index[bad_cid].get("path")
                        if chunk_path and os.path.exists(chunk_path):
                            os.remove(chunk_path)
                        del index[bad_cid]
                persist_index()
            # Send corruption report to namenode
            try:
                requests.post(f"http://{NAMENODE_IP}:{NAMENODE_PORT}/report_corruption",
                              json={"node_id": NODE_ID, "bad_chunks": bad}, timeout=5)
            except Exception:
                pass
        time.sleep(interval)


# ============================================================
# Main Entry
# ============================================================
if __name__ == "__main__":
    logger.info("=" * 70)
    logger.info(f"Starting DataNode {NODE_ID} (Primary Storage Node)")
    logger.info("=" * 70)
    logger.info(f"Listening on {FLASK_HOST}:{FLASK_PORT}")
    logger.info(f"Storage directory: {STORAGE}")
    logger.info(f"NameNode: {NAMENODE_IP}:{NAMENODE_PORT}")
    logger.info(f"Secondaries: {len(SECONDARIES)} nodes configured")
    
    try:
        register_url = f"http://{NAMENODE_IP}:{NAMENODE_PORT}/register_datanode"
        register_payload = {
            "node_id": NODE_ID,
            "ip": MY_IP,
            "port": FLASK_PORT,
            "role": "datanode_primary",
            "stored_chunks": len(index)
        }
        requests.post(register_url, json=register_payload, timeout=5)
        logger.info(f"✓ Registered with NameNode at {NAMENODE_IP}:{NAMENODE_PORT}")
    except Exception as e:
        logger.warning(f"⚠ Could not register with NameNode: {e}")
        logger.warning("Will retry registration via heartbeat...")
    
    threading.Thread(target=validate_on_startup, daemon=True).start()
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    threading.Thread(target=periodic_corruption_check, args=(60,), daemon=True).start()
    
    logger.info("=" * 70)
    logger.info(f"{NODE_ID} initialized and ready")
    logger.info("=" * 70)
    
    app.run(host=FLASK_HOST, port=FLASK_PORT, threaded=True, debug=C.get("debug", False))
