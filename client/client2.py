import os
import json
import time
from flask import Flask, request, render_template, jsonify
import requests
from datetime import datetime
from flask import Response

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config_client.json")
try:
    with open(CONFIG_PATH, "r") as f:
        CONFIG = json.load(f)
except Exception:
    CONFIG = {}

NAMENODE = {
    "ip": CONFIG.get("namenode_ip", CONFIG.get("namenode", {}).get("ip", CONFIG.get("ip", "127.0.0.1"))),
    "port": CONFIG.get("namenode_port", CONFIG.get("namenode", {}).get("port", 5000))
}

# Simple in-memory event log
event_log = []
MAX_LOG_EVENTS = 100

app = Flask(__name__, template_folder="templates")

def add_event(event_type, message):
    """Add an event to the log"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    event_log.append({"timestamp": timestamp, "type": event_type, "message": message})
    if len(event_log) > MAX_LOG_EVENTS:
        event_log.pop(0)

@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")

@app.route("/metadata", methods=["GET"])
def client_metadata():
    try:
        r = requests.get(f"http://{NAMENODE['ip']}:{NAMENODE['port']}/metadata", timeout=5)
        meta = r.json() if r.ok else {"error": r.text}
    except Exception as e:
        meta = {"error": str(e)}
    return jsonify(meta)

@app.route("/events", methods=["GET"])
def get_events():
    """Return recent system events"""
    return jsonify({"events": event_log})

@app.route("/system-info", methods=["GET"])
def system_info():
    """Return system configuration and uptime"""
    try:
        r = requests.get(f"http://{NAMENODE['ip']}:{NAMENODE['port']}/metadata", timeout=5)
        meta = r.json()
        
        # Calculate uptime for each node
        now = time.time()
        datanodes_info = {}
        for node_id, info in meta.get("datanodes", {}).items():
            last_hb = info.get("last_heartbeat", 0)
            time_since_hb = now - last_hb
            datanodes_info[node_id] = {
                "ip": info.get("ip"),
                "port": info.get("port"),
                "last_heartbeat": last_hb,
                "time_since_heartbeat_sec": int(time_since_hb),
                "chunk_count": info.get("chunk_count", 0)
            }
        
        return jsonify({
            "namenode": {"ip": NAMENODE["ip"], "port": NAMENODE["port"]},
            "datanodes": datanodes_info,
            "replication_factor": meta.get("replication_factor", 2),
            "chunk_size": meta.get("chunk_size", 5242880)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/upload", methods=["POST"])
def upload():
    """Upload a file from client to NameNode with retry and streaming support."""
    f = request.files.get("file")
    if not f:
        return jsonify({"status": "error", "msg": "no file provided"}), 400

    filename = f.filename
    data = {"filename": filename}

    # --- Important: Don't read() the file, just pass it as a stream ---
    f.stream.seek(0)
    files = {"file": (filename, f)}

    namenode_url = f"http://{NAMENODE['ip']}:{NAMENODE['port']}/upload"

    try:
        add_event("upload_start", f"Uploading {filename}...")
        success = False
        backoff = 1

        # Up to 3 attempts with exponential backoff
        for attempt in range(1, 4):
            try:
                r = requests.post(namenode_url, files=files, data=data, timeout=500)
                if r.status_code == 200:
                    success = True
                    result = r.json() if r.headers.get("Content-Type", "").startswith("application/json") else {"status": "ok"}
                    add_event("upload_complete", f"✓ Successfully uploaded {filename}")
                    return jsonify(result), 200
                else:
                    add_event("upload_error", f"Attempt {attempt}: HTTP {r.status_code} – {r.text[:200]}")
            except requests.exceptions.Timeout:
                add_event("upload_error", f"Attempt {attempt}: Timeout while uploading {filename}")
            except requests.exceptions.ConnectionError as e:
                add_event("upload_error", f"Attempt {attempt}: Connection error: {e}")
            except Exception as e:
                add_event("upload_error", f"Attempt {attempt}: Unexpected error: {str(e)}")

            if attempt < 3:
                time.sleep(backoff)
                backoff = min(backoff * 2, 8)  # exponential backoff up to 8 seconds

        if not success:
            add_event("upload_error", f"✗ Failed to upload {filename} after multiple retries")
            return jsonify({"status": "error", "msg": "upload failed after retries"}), 500

    except Exception as e:
        add_event("upload_error", f"✗ Upload fatal error: {str(e)}")
        return jsonify({"status": "error", "msg": str(e)}), 500


@app.route("/download", methods=["POST"])
def download():
    filename = request.form.get("filename")
    if not filename:
        return jsonify({"status":"error","msg":"missing filename"}), 400

    try:
        add_event("download_start", f"Downloading {filename}...")
        r = requests.get(f"http://{NAMENODE['ip']}:{NAMENODE['port']}/metadata", timeout=5)
        meta = r.json()
    except Exception as e:
        add_event("download_error", f"✗ Cannot fetch metadata: {str(e)}")
        return jsonify({"status":"error","msg":"cannot fetch metadata", "error": str(e)}), 500

    if filename not in meta.get("files", {}):
        add_event("download_error", f"✗ File not found: {filename}")
        return jsonify({"status":"error", "msg":"file not found"}), 404

    fmeta = meta["files"][filename]
    download_path = os.path.join(BASE_DIR, "downloads", filename)
    
    os.makedirs(os.path.dirname(download_path), exist_ok=True)

    def generate_and_save():
        with open(download_path, "wb") as out_f:
            for chunk_id, hosts in fmeta["chunks"].items():
                found = False
                for host in hosts:
                    dn = meta["datanodes"].get(host)
                    if not dn:
                        continue
                    try:
                        with requests.get(
                            f"http://{dn['ip']}:{dn['port']}/get_chunk",
                            params={"chunk_id": chunk_id},
                            stream=True,
                            timeout=600
                        ) as r:
                            r.raise_for_status()
                            for packet in r.iter_content(chunk_size=65536):
                                if packet:
                                    out_f.write(packet)
                                    yield packet
                            found = True
                            break
                    except Exception as e:
                        continue
                if not found:
                    add_event("download_error", f"✗ Chunk {chunk_id} unreachable")
                    msg = f"Chunk {chunk_id} unreachable".encode()
                    out_f.write(msg)
                    yield msg

    add_event("download_complete", f"✓ Downloaded {filename}")
    return Response(
        generate_and_save(),
        mimetype="application/octet-stream",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

@app.route("/delete-file", methods=["POST"])
def delete_file():
    """Delete a file (placeholder - implement in namenode if needed)"""
    filename = request.form.get("filename")
    if not filename:
        return jsonify({"status":"error","msg":"missing filename"}), 400
    
    try:
        # Forward to namenode if it has a delete endpoint
        r = requests.post(f"http://{NAMENODE['ip']}:{NAMENODE['port']}/delete_file", 
                         json={"filename": filename}, timeout=10)
        add_event("file_deleted", f"Deleted {filename}")
        return jsonify(r.json()), r.status_code
    except Exception as e:
        add_event("delete_error", f"Error deleting {filename}: {str(e)}")
        return jsonify({"status":"error","msg": str(e)}), 500

if __name__ == "__main__":
    app.run(host=CONFIG.get("ip", "0.0.0.0"), port=5001, debug=CONFIG.get("debug", False))
