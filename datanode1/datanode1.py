#!/usr/bin/env python3
"""
DataNode Secondary (Node 4) - Storage Node B (Updated, hardened)

Improvements included:
 - Robust index load with backup on corruption
 - Atomic index persistence under lock
 - Atomic chunk writes for store and replicate (tmp -> verify -> rename)
 - notify_namenode_stored with retries and linear backoff
 - try_register_with_namenode with retries at startup
 - Heartbeat loop uses shutdown_event and supports re-registration attempts
 - Graceful shutdown (SIGINT/SIGTERM): persists index and stops background threads
 - Periodic corruption check and startup validation that remove/report bad chunks
 - Cleanup of stale .tmp files and orphaned chunks
 - delete_chunk, store_chunk, get_chunk, replicate_here, verify_chunk, list_chunks endpoints
"""

import os
import json
import time
import threading
import hashlib
import logging
import re
import signal
import sys
from time import perf_counter
from flask import Flask, request, jsonify, send_file
import requests

# ============================================================
# Setup & Configuration
# ============================================================
BASE = os.path.dirname(os.path.abspath(__file__))
STORAGE = os.path.join(BASE, "storage")
INDEX = os.path.join(BASE, "chunks_index_secondary.json")
CONFIG_PATH = os.path.join(BASE, "config_secondary.json")
os.makedirs(STORAGE, exist_ok=True)

# Default config (will be merged with config file if present)
DEFAULT_CONFIG = {
    "node_id": "node4_datanodeB",
    "ip": "0.0.0.0",
    "port": 7001,
    "namenode_ip": "127.0.0.1",
    "namenode_port": 5000,
    "heartbeat_interval": 5,
    "debug": True,
    "role": "datanode_secondary",
    "tmp_age_threshold": 60  # seconds for startup tmp cleanup / periodic
}

# Load config (fallback to defaults)
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
ROLE = C.get("role", "datanode_secondary")
TMP_AGE_THRESHOLD = C.get("tmp_age_threshold", 60)

FLASK_HOST = "0.0.0.0"
FLASK_PORT = MY_PORT

# Globals for registration/heartbeat and shutdown
REGISTER_URL = None
REGISTER_PAYLOAD = None
shutdown_event = threading.Event()

# ============================================================
# Logging setup
# ============================================================
log_level = logging.DEBUG if C.get("debug", False) else logging.INFO
logging.basicConfig(
    level=log_level,
    format=f'%(asctime)s [{NODE_ID}] %(levelname)s: %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(BASE, "datanode_secondary.log"), mode='a')
    ]
)
logger = logging.getLogger(__name__)

# ============================================================
# Flask App Setup
# ============================================================
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024  # 200MB upload cap

# ============================================================
# Index handling (robust load & atomic persist)
# ============================================================
index_lock = threading.RLock()

if os.path.exists(INDEX):
    try:
        with open(INDEX) as f:
            index = json.load(f)
            if not isinstance(index, dict):
                raise ValueError("index file does not contain a dict")
    except Exception as e:
        logger.error(f"Index file corrupted or invalid, moving to {INDEX}.bak ({e})")
        try:
            os.rename(INDEX, INDEX + ".bak")
        except Exception:
            logger.exception("Failed to rename corrupted index file")
        index = {}
else:
    index = {}

def persist_index():
    """Persist chunk index safely under index_lock (atomic rename)."""
    tmp = INDEX + ".tmp"
    try:
        with index_lock:
            with open(tmp, "w") as f:
                json.dump(index, f, indent=2)
            os.replace(tmp, INDEX)
    except Exception as e:
        logger.error(f"Error persisting index: {e}")

# ============================================================
# Helpers / Validation
# ============================================================
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
    except Exception:
        return False

def cleanup_orphaned_chunks(tmp_age_threshold=TMP_AGE_THRESHOLD):
    """Remove files on disk that are not present in index and remove old .tmp files."""
    logger.info("Cleaning up orphaned chunks and stale tmp files...")
    try:
        files_on_disk = set(os.listdir(STORAGE))
        with index_lock:
            indexed_files = set(os.path.basename(rec.get("path","")) for rec in index.values() if rec.get("path"))
        orphaned = files_on_disk - indexed_files
        for orphan in orphaned:
            orphan_path = os.path.join(STORAGE, orphan)
            try:
                if os.path.isfile(orphan_path):
                    os.remove(orphan_path)
                    logger.info(f"Removed orphaned chunk: {orphan}")
            except Exception as e:
                logger.error(f"Failed to remove orphan {orphan}: {e}")
        # remove stale .tmp files older than threshold (seconds)
        now = time.time()
        for fname in os.listdir(STORAGE):
            if fname.endswith(".tmp"):
                fp = os.path.join(STORAGE, fname)
                try:
                    age = now - os.path.getmtime(fp)
                    if age > tmp_age_threshold:
                        os.remove(fp)
                        logger.info(f"Removed old tmp file: {fname}")
                except Exception as e:
                    logger.debug(f"Failed cleaning tmp {fname}: {e}")
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

def notify_namenode_stored(chunk_id, checksum, retries=3):
    """Notify NameNode that we've stored a chunk. Retries with linear backoff."""
    url = f"http://{NAMENODE_IP}:{NAMENODE_PORT}/datanode_stored"
    for attempt in range(1, retries + 1):
        try:
            r = requests.post(url, json={"node_id": NODE_ID, "chunk_id": chunk_id, "checksum": checksum}, timeout=5)
            if r.status_code == 200:
                logger.debug(f"Notified NameNode about {chunk_id} (attempt {attempt})")
                return True
            else:
                logger.warning(f"Notify returned {r.status_code} (attempt {attempt})")
        except Exception as e:
            logger.debug(f"Notify attempt {attempt} failed: {e}")
        time.sleep(attempt)  # linear backoff
    logger.warning(f"Failed to notify NameNode about {chunk_id} after {retries} attempts")
    return False

def try_register_with_namenode(register_url, payload, retries=3):
    """Attempt registration with NameNode with retries/backoff."""
    for attempt in range(1, retries + 1):
        try:
            r = requests.post(register_url, json=payload, timeout=5)
            if r.status_code == 200:
                logger.info("Registered with NameNode successfully")
                return True
            else:
                logger.warning(f"Register returned {r.status_code} (attempt {attempt})")
        except Exception as e:
            logger.debug(f"Register attempt {attempt} failed: {e}")
        time.sleep(attempt)
    logger.warning("Could not register with NameNode after retries")
    return False

# ============================================================
# Routes / API
# ============================================================
@app.route("/", methods=["GET"])
def index_route():
    return jsonify({"status": "ok", "node_id": NODE_ID}), 200

@app.route("/health", methods=["GET"])
def health_check():
    try:
        with index_lock:
            stored = len(index)
        return jsonify({
            "status": "healthy",
            "node_id": NODE_ID,
            "port": MY_PORT,
            "role": ROLE,
            "stored_chunks": stored,
            "uptime": time.time()
        }), 200
    except Exception as e:
        logger.error(f"Health check error: {e}")
        return jsonify({"status": "error", "msg": str(e)}), 500

@app.route("/store_chunk", methods=["POST"])
def store_chunk():
    """Store chunk safely with validation (atomic tmp -> rename)."""
    logger.info("store_chunk called")
    try:
        chunk_file = request.files.get("chunk")
        chunk_id = request.form.get("chunk_id") or (chunk_file.filename if chunk_file else None)
        origin = request.form.get("origin", "client")
        is_replica = request.form.get("is_replica", "false").lower() == "true"
        declared_size = request.form.get("chunk_size")
        if declared_size:
            try:
                declared_size = int(declared_size)
            except ValueError:
                logger.error(f"Invalid chunk_size: {declared_size}")
                return jsonify({"status": "error", "msg": "invalid chunk_size"}), 400

        if not chunk_file or not chunk_id:
            logger.error("Missing chunk or chunk_id")
            return jsonify({"status": "error", "msg": "missing chunk or chunk_id"}), 400

        if not validate_chunk_id(chunk_id):
            logger.error(f"Invalid chunk_id: {chunk_id}")
            return jsonify({"status": "error", "msg": "invalid chunk_id format"}), 400

        t0 = perf_counter()
        temp_path = None
        try:
            temp_path = os.path.join(STORAGE, f"{chunk_id}.tmp")
            final_path = os.path.join(STORAGE, chunk_id)

            save_start = perf_counter()
            chunk_file.save(temp_path)
            save_end = perf_counter()

            actual_size = os.path.getsize(temp_path)
            if declared_size and actual_size != declared_size:
                os.remove(temp_path)
                logger.warning(f"Size mismatch for {chunk_id}: declared={declared_size}, actual={actual_size}")
                return jsonify({"status": "error", "msg": f"size mismatch: expected {declared_size}, got {actual_size}"}), 400

            checksum = sha256_stream(temp_path)
            if not checksum:
                os.remove(temp_path)
                logger.error("Checksum computation failed")
                return jsonify({"status": "error", "msg": "checksum computation failed"}), 500
            cs_end = perf_counter()

            with index_lock:
                if os.path.exists(final_path):
                    try:
                        os.remove(final_path)
                    except Exception:
                        logger.debug("Could not remove existing final file before rename")
                os.rename(temp_path, final_path)
                temp_path = None
                index[chunk_id] = {
                    "path": final_path,
                    "checksum": checksum,
                    "origin": origin,
                    "is_replica": is_replica,
                    "stored_at": time.time(),
                    "size": actual_size
                }
                persist_index()

            logger.info(f"Stored chunk {chunk_id} checksum={checksum} size={actual_size} "
                        f"(save={save_end - save_start:.3f}s, checksum={cs_end - save_end:.3f}s, total={perf_counter() - t0:.3f}s)")

            threading.Thread(target=notify_namenode_stored, args=(chunk_id, checksum), daemon=True).start()
            return jsonify({"status": "ok", "chunk_id": chunk_id, "checksum": checksum, "size": actual_size}), 200

        except Exception as e:
            logger.exception(f"store_chunk exception: {e}")
            if temp_path and os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception:
                    pass
            return jsonify({"status": "error", "msg": str(e)}), 500

    except Exception as e:
        logger.exception(f"store_chunk outer exception: {e}")
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
        logger.warning(f"get_chunk not found: {chunk_id}")
        return jsonify({"status": "error", "msg": "not found"}), 404

    chunk_path = rec.get("path")
    if not chunk_path or not validate_path_in_storage(chunk_path):
        logger.error(f"Invalid path for chunk {chunk_id}: {chunk_path}")
        return jsonify({"status": "error", "msg": "invalid chunk path"}), 500

    if not os.path.exists(chunk_path):
        logger.error(f"Chunk file not found on disk: {chunk_id}")
        with index_lock:
            if chunk_id in index:
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
    with index_lock:
        chunks_list = [
            {
                "chunk_id": cid,
                "size": rec.get("size", 0),
                "checksum": rec.get("checksum"),
                "is_replica": rec.get("is_replica", False),
                "stored_at": rec.get("stored_at")
            }
            for cid, rec in index.items()
        ]
    return jsonify({"chunks": chunks_list, "total": len(chunks_list)}), 200

@app.route("/delete_chunk", methods=["POST"])
def delete_chunk():
    """Delete a chunk from storage and index. Expects JSON: {"chunk_id": "..."}."""
    try:
        body = request.get_json() or {}
        chunk_id = body.get("chunk_id")
        if not chunk_id:
            return jsonify({"status": "error", "msg": "missing chunk_id"}), 400

        if not validate_chunk_id(chunk_id):
            return jsonify({"status": "error", "msg": "invalid chunk_id"}), 400

        with index_lock:
            rec = index.get(chunk_id)
            if not rec:
                return jsonify({"status": "error", "msg": "not found"}), 404

            path = rec.get("path")
            try:
                if path and os.path.exists(path):
                    os.remove(path)
            except Exception as e:
                logger.warning(f"Failed to remove chunk file {path}: {e}")

            del index[chunk_id]
            persist_index()

        logger.info(f"Deleted chunk {chunk_id} as requested")
        return jsonify({"status": "ok", "chunk_id": chunk_id}), 200

    except Exception as e:
        logger.exception(f"delete_chunk exception: {e}")
        return jsonify({"status": "error", "msg": str(e)}), 500

@app.route("/replicate_here", methods=["POST"])
def replicate_here():
    """Accept replica chunks from primary datanode (atomic, verified)."""
    logger.info("replicate_here called")
    try:
        chunk_file = request.files.get("chunk")
        chunk_id = request.form.get("chunk_id")
        origin = request.form.get("origin", "unknown")
        is_replica = request.form.get("is_replica", "false").lower() == "true"
        declared_size = request.form.get("chunk_size")
        if declared_size:
            try:
                declared_size = int(declared_size)
            except ValueError:
                return jsonify({"status":"error","msg":"invalid chunk_size"}), 400

        if not chunk_file or not chunk_id:
            return jsonify({"status":"error","msg":"missing chunk or chunk_id"}), 400
        if not validate_chunk_id(chunk_id):
            return jsonify({"status":"error","msg":"invalid chunk_id format"}), 400

        tmp_path = os.path.join(STORAGE, f"{chunk_id}.tmp")
        final_path = os.path.join(STORAGE, chunk_id)
        try:
            chunk_file.save(tmp_path)
            actual_size = os.path.getsize(tmp_path)
            if declared_size and actual_size != declared_size:
                os.remove(tmp_path)
                logger.warning(f"Replica size mismatch for {chunk_id}: declared={declared_size}, actual={actual_size}")
                return jsonify({"status":"error","msg":"size mismatch"}), 400

            checksum = sha256_stream(tmp_path)
            if not checksum:
                os.remove(tmp_path)
                logger.error("Checksum failed for replica")
                return jsonify({"status":"error","msg":"checksum failed"}), 500

            with index_lock:
                if os.path.exists(final_path):
                    try:
                        os.remove(final_path)
                    except Exception:
                        logger.debug("Could not remove existing final file before rename")
                os.rename(tmp_path, final_path)
                index[chunk_id] = {
                    "path": final_path,
                    "checksum": checksum,
                    "origin": origin,
                    "is_replica": is_replica,
                    "stored_at": time.time(),
                    "size": actual_size
                }
                persist_index()

            threading.Thread(target=notify_namenode_stored, args=(chunk_id, checksum), daemon=True).start()
            logger.info(f"Replica received: {chunk_id} checksum={checksum}")
            return jsonify({"status":"ok","chunk_id":chunk_id,"checksum":checksum}), 200

        except Exception as e:
            logger.exception("replicate_here error")
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass
            return jsonify({"status":"error","msg":str(e)}), 500

    except Exception as e:
        logger.exception("replicate_here outer")
        return jsonify({"status":"error","msg":str(e)}), 500

@app.route("/verify_chunk", methods=["GET"])
def verify_chunk():
    chunk_id = request.args.get("chunk_id")
    if not chunk_id:
        return jsonify({"status":"error","msg":"missing chunk_id"}), 400

    with index_lock:
        rec = index.get(chunk_id)
    if not rec:
        return jsonify({"status":"error","msg":"not found"}), 404

    chunk_path = rec.get("path")
    if not chunk_path or not os.path.exists(chunk_path):
        return jsonify({"status":"error","msg":"file missing on disk"}), 500

    current_checksum = sha256_stream(chunk_path)
    stored_checksum = rec.get("checksum")
    if current_checksum == stored_checksum:
        return jsonify({"status":"ok","chunk_id":chunk_id,"checksum":current_checksum,"verified":True}), 200
    else:
        return jsonify({"status":"error","msg":"checksum mismatch","expected":stored_checksum,"actual":current_checksum,"verified":False}), 500

# ============================================================
# Heartbeat Loop (with re-registration)
# ============================================================
def heartbeat_loop():
    """Send periodic heartbeats to NameNode. Re-register after repeated failures."""
    url = f"http://{NAMENODE_IP}:{NAMENODE_PORT}/heartbeat"
    consecutive_failures = 0
    re_register_threshold = 6

    while not shutdown_event.is_set():
        try:
            with index_lock:
                stored_chunks = len(index)
            payload = {
                "node_id": NODE_ID,
                "ip": MY_IP,
                "port": FLASK_PORT,
                "role": ROLE,
                "stored_chunks": stored_chunks,
                "timestamp": time.time()
            }
            response = requests.post(url, json=payload, timeout=5)
            if response.status_code == 200:
                logger.debug("Heartbeat sent successfully")
                consecutive_failures = 0
            else:
                logger.warning(f"Heartbeat returned status {response.status_code}")
                consecutive_failures += 1
        except Exception as e:
            logger.warning(f"Heartbeat error: {e}")
            consecutive_failures += 1

        if consecutive_failures >= re_register_threshold and REGISTER_URL and REGISTER_PAYLOAD:
            logger.warning("Heartbeat failing repeatedly; attempting re-registration with NameNode")
            try_register_with_namenode(REGISTER_URL, REGISTER_PAYLOAD, retries=2)

        if consecutive_failures == 0:
            sleep_time = HEARTBEAT_INTERVAL
        else:
            sleep_time = min(HEARTBEAT_INTERVAL * (2 ** (consecutive_failures - 1)), 30)

        logger.debug(f"Next heartbeat in {sleep_time}s (failures: {consecutive_failures})")
        shutdown_event.wait(timeout=sleep_time)

# ============================================================
# Validation on Startup & Periodic Corruption Check
# ============================================================
def validate_on_startup():
    """Validate stored chunks on startup (remove bad chunks and report)."""
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
                except Exception:
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

def periodic_corruption_check(interval=60):
    logger.info(f"Starting periodic corruption check every {interval}s")
    while not shutdown_event.is_set():
        bad = []
        with index_lock:
            for cid, rec in list(index.items()):
                try:
                    chunk_path = rec.get("path")
                    if not chunk_path or not os.path.exists(chunk_path):
                        bad.append(cid)
                    else:
                        current_checksum = sha256_stream(chunk_path)
                        stored_checksum = rec.get("checksum")
                        if current_checksum != stored_checksum:
                            bad.append(cid)
                except Exception:
                    logger.exception(f"Error checking {cid}")
        if bad:
            logger.warning(f"Periodic check found {len(bad)} bad chunks: {bad}")
            with index_lock:
                for bad_cid in bad:
                    if bad_cid in index:
                        chunk_path = index[bad_cid].get("path")
                        if chunk_path and os.path.exists(chunk_path):
                            try:
                                os.remove(chunk_path)
                            except Exception:
                                pass
                        del index[bad_cid]
                persist_index()
            try:
                requests.post(f"http://{NAMENODE_IP}:{NAMENODE_PORT}/report_corruption",
                              json={"node_id": NODE_ID, "bad_chunks": bad}, timeout=5)
            except Exception:
                logger.debug("Failed to report corruption during periodic check")
        shutdown_event.wait(timeout=interval)

# ============================================================
# Graceful shutdown (signal handlers)
# ============================================================
def graceful_shutdown(signum, frame):
    logger.info(f"Received signal {signum}; persisting index and shutting down")
    try:
        persist_index()
    except Exception:
        pass
    shutdown_event.set()
    # give threads a moment to exit
    time.sleep(0.5)
    sys.exit(0)

signal.signal(signal.SIGINT, graceful_shutdown)
signal.signal(signal.SIGTERM, graceful_shutdown)

# ============================================================
# Main entry
# ============================================================
if __name__ == "__main__":
    logger.info("=" * 70)
    logger.info(f"Starting DataNode {NODE_ID} (Secondary Storage Node - Replication)")
    logger.info("=" * 70)
    logger.info(f"Listening on {FLASK_HOST}:{FLASK_PORT}")
    logger.info(f"Storage directory: {STORAGE}")
    logger.info(f"NameNode: {NAMENODE_IP}:{NAMENODE_PORT}")
    logger.info(f"Role: {ROLE}")

    # Pre-start cleanup: remove stale .tmp files and orphaned chunks
    cleanup_orphaned_chunks(tmp_age_threshold=5)  # small threshold at startup to remove leftover tmps quickly

    # Prepare registration variables for heartbeat thread
    REGISTER_URL = f"http://{NAMENODE_IP}:{NAMENODE_PORT}/register_datanode"
    REGISTER_PAYLOAD = {
        "node_id": NODE_ID,
        "ip": MY_IP,
        "port": FLASK_PORT,
        "role": ROLE,
        "stored_chunks": len(index)
    }

    # Try registering once at startup (with retries)
    try_register_with_namenode(REGISTER_URL, REGISTER_PAYLOAD, retries=3)

    # Start validation & background threads
    threading.Thread(target=validate_on_startup, daemon=True).start()
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    threading.Thread(target=periodic_corruption_check, args=(60,), daemon=True).start()

    logger.info("=" * 70)
    logger.info(f"{NODE_ID} initialized and ready (Secondary)")
    logger.info("=" * 70)
    logger.info("Available endpoints:")
    logger.info("  GET  /health")
    logger.info("  POST /store_chunk")
    logger.info("  GET  /get_chunk?chunk_id=...")
    logger.info("  POST /replicate_here")
    logger.info("  POST /delete_chunk")
    logger.info("  GET  /list_chunks")
    logger.info("  GET  /verify_chunk?chunk_id=...")

    # Start Flask (blocks)
    try:
        logger.info(f"Starting Flask server on {FLASK_HOST}:{FLASK_PORT}...")
        app.run(host=FLASK_HOST, port=FLASK_PORT, threaded=True, debug=False)
    except Exception:
        logger.exception("Flask server terminated unexpectedly")
    finally:
        try:
            persist_index()
        except Exception:
            pass
