# client_ui.py
import os
import json
from flask import Flask, request, render_template, jsonify
import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config_client.json")
try:
    with open(CONFIG_PATH, "r") as f:
        CONFIG = json.load(f)
except Exception:
    CONFIG = {}

# tolerate several possible config key names
NAMENODE = {
    "ip": CONFIG.get("namenode_ip", CONFIG.get("namenode", {}).get("ip", CONFIG.get("ip", "127.0.0.1"))),
    "port": CONFIG.get("namenode_port", CONFIG.get("namenode", {}).get("port", 5000))
}

app = Flask(__name__, template_folder="templates")

@app.route("/", methods=["GET"])
def index():
    try:
        r = requests.get(f"http://{NAMENODE['ip']}:{NAMENODE['port']}/metadata", timeout=5)
        meta = r.json() if r.ok else {"error": r.text}
    except Exception as e:
        meta = {"error": str(e)}
    return render_template("index.html", metadata=meta)

@app.route("/upload", methods=["POST"])
def upload():
    f = request.files.get("file")
    if not f:
        return "no file", 400
    data = {"filename": f.filename}
    files = {"file": (f.filename, f.read())}
    try:
        r = requests.post(f"http://{NAMENODE['ip']}:{NAMENODE['port']}/upload", files=files, data=data, timeout=60)
        return jsonify(r.json() if r.headers.get("Content-Type","").startswith("application/json") else {"status":"ok","raw": r.text}), r.status_code
    except Exception as e:
        return jsonify({"status":"error","msg": str(e)}), 500

@app.route("/download", methods=["POST"])
def download():
    filename = request.form.get("filename")
    if not filename:
        return jsonify({"status":"error","msg":"missing filename"}), 400
    try:
        r = requests.get(f"http://{NAMENODE['ip']}:{NAMENODE['port']}/metadata", timeout=5)
        meta = r.json()
    except Exception as e:
        return jsonify({"status":"error","msg":"cannot fetch metadata", "error": str(e)}), 500
    if filename not in meta.get("files", {}):
        return jsonify({"status":"error", "msg":"file not found"}), 404
    fmeta = meta["files"][filename]
    chunks = []
    for chunk_id, hosts in fmeta["chunks"].items():
        content = None
        for host in hosts:
            dn = meta["datanodes"].get(host)
            if not dn:
                continue
            try:
                r = requests.get(f"http://{dn['ip']}:{dn['port']}/get_chunk", params={"chunk_id": chunk_id}, timeout=10)
                if r.status_code == 200:
                    content = r.content
                    break
            except Exception:
                continue
        if content is None:
            return jsonify({"status":"error", "msg": f"chunk {chunk_id} unreachable"}), 500
        chunks.append(content)
    data = b"".join(chunks)
    return (data, 200, {
        "Content-Type": "application/octet-stream",
        "Content-Disposition": f"attachment; filename={filename}"
    })

if __name__ == "__main__":
    app.run(host=CONFIG.get("ip", "0.0.0.0"), port=5001, debug=CONFIG.get("debug", False))
