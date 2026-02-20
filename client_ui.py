
import os, io, math, hashlib, struct, socket, json
from flask import Flask, request, render_template_string, send_file, jsonify
import requests, argparse

# namenode ip
parser = argparse.ArgumentParser()
parser.add_argument("--namenode-api", default="http://172.25.5.104:8000",
                    help="URL of the Namenode API (including http:// and port)")
parser.add_argument("--port", type=int, default=7000)
args = parser.parse_args()

NAMENODE_API = args.namenode_api.rstrip("/")
CHUNK_SIZE = 2 * 1024 * 1024 
app = Flask(__name__)

# send msg
def send_msg(sock, payload_bytes):
    sock.sendall(struct.pack("!I", len(payload_bytes)) + payload_bytes)
    a
#recv msg
def recv_msg(sock):
    raw = b""
    while len(raw) < 4:
        part = sock.recv(4 - len(raw))
        if not part:
            return None
        raw += part
    (length,) = struct.unpack("!I", raw)
    data = b""
    while len(data) < length:
        chunk = sock.recv(length - len(data))
        if not chunk:
            break
        data += chunk
    return data if len(data) == length else None

# Routes
@app.route("/")
def index():
    html = """
    <!doctype html>
    <html>
    <head>
      <title>Mini HDFS Client</title>
      <style>
        body {
          font-family: 'Segoe UI', sans-serif;
          background: #f5f7fa;
          color: #222;
          margin: 0;
          padding: 0;
        }
        header {
          background: linear-gradient(90deg, #3b82f6, #06b6d4);
          color: white;
          text-align: center;
          padding: 20px;
          font-size: 26px;
          letter-spacing: 1px;
          box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        main {
          display: flex;
          justify-content: space-around;
          flex-wrap: wrap;
          padding: 20px;
        }
        .card {
          background: white;
          border-radius: 12px;
          box-shadow: 0 2px 8px rgba(0,0,0,0.1);
          padding: 20px;
          margin: 15px;
          width: 45%;
          min-width: 400px;
        }
        h2 { color: #2563eb; border-bottom: 2px solid #93c5fd; padding-bottom: 5px; }
        table { border-collapse: collapse; width: 100%; margin-top: 10px; }
        th, td {
          border: 1px solid #ddd;
          padding: 8px;
          text-align: center;
        }
        th { background: #e0f2fe; color: #1e3a8a; }
        tr:nth-child(even) { background: #f9fafb; }
        input[type=file], input[type=submit], select {
          margin-top: 10px;
          padding: 8px;
          border-radius: 8px;
          border: 1px solid #cbd5e1;
        }
        input[type=submit] {
          background: #3b82f6;
          color: white;
          border: none;
          cursor: pointer;
        }
        input[type=submit]:hover { background: #2563eb; }
        a { color: #2563eb; text-decoration: none; }
        a:hover { text-decoration: underline; }
      </style>
    </head>
    <body>
      <header>Mini HDFS Client Dashboard</header>
      <main>
        <div class="card">
          <h2>Upload File</h2>
          <form action="/upload" method="post" enctype="multipart/form-data">
            <input type="file" name="file" required/>
            <input type="submit" value="Upload">
          </form>
          <h2>Stored Files</h2>
          <div id="files"></div>
        </div>
        <div class="card">
          <h2>Cluster Overview</h2>
          <div id="nodes"></div>
          <h3 style="margin-top:15px;">Chunk Map (select file)</h3>
          <select id="fileSelect" onchange="showChunkMap()"></select>
          <div id="chunkmap"></div>
        </div>
      </main>
      <script>
      async function loadFiles() {
        let r = await fetch('/list_files');
        let js = await r.json();
        let html = '<ul>';
        js.forEach(f => {
          html += `<li>${f} - <a href="/download/${encodeURIComponent(f)}">Download</a></li>`;
        });
        html += '</ul>';
        document.getElementById('files').innerHTML = html;
        let sel = document.getElementById('fileSelect');
        sel.innerHTML = '<option value="">-- choose file --</option>';
        js.forEach(f => {
          let o = document.createElement('option');
          o.value = f;
          o.textContent = f;
          sel.appendChild(o);
        });
      }
      async function loadNodes() {
        let r = await fetch('/datanodes');
        let js = await r.json();
        let html = '<table><tr><th>ID</th><th>Host</th><th>Port</th><th>Status</th></tr>';
        Object.entries(js).forEach(([id,v])=>{
          const color = v.alive ? '#16a34a' : '#dc2626';
          const status = v.alive ? 'Online' : 'Offline';
          html += `<tr><td>${id}</td><td>${v.host}</td><td>${v.port}</td><td style="color:${color};font-weight:bold;">${status}</td></tr>`;
        });
        html += '</table>';
        document.getElementById('nodes').innerHTML = html;
      }
      async function showChunkMap() {
        let f = document.getElementById('fileSelect').value;
        if (!f) { document.getElementById('chunkmap').innerHTML = ''; return; }
        let r = await fetch('/file_info/' + encodeURIComponent(f));
        if (r.status !== 200) {
          document.getElementById('chunkmap').innerHTML = 'File not found';
          return;
        }
        let info = await r.json();
        let dns = await (await fetch('/datanodes')).json();
        let html = '<table><tr><th>Chunk #</th><th>Size</th><th>Replicas (Host:Port)</th></tr>';
        info.chunks.forEach(ch => {
          html += `<tr><td>${ch.index}</td><td>${ch.size} bytes</td><td>`;
          ch.replicas.forEach(rep => {
            const alive = dns[rep.id] ? dns[rep.id].alive : false;
            const color = alive ? '#16a34a' : '#dc2626';
            html += `<span style="color:${color};">${rep.host}:${rep.port}</span><br>`;
          });
          html += '</td></tr>';
        });
        html += '</table>';
        document.getElementById('chunkmap').innerHTML = html;
      }
      window.onload = async () => {
        await loadFiles(); await loadNodes();
        setInterval(loadNodes, 4000);
      };
      </script>
    </body>
    </html>
    """
    return render_template_string(html)

@app.route("/list_files")
def list_files():
    r = requests.get(NAMENODE_API + "/list_files")
    return jsonify(r.json())

@app.route("/datanodes")
def datanodes():
    r = requests.get(NAMENODE_API + "/datanodes")
    return jsonify(r.json())

@app.route("/file_info/<path:filename>")
def file_info(filename):
    r = requests.get(NAMENODE_API + "/file_info/" + filename)
    return (r.text, r.status_code, r.headers.items())

@app.route("/upload", methods=["POST"])
def upload():
    f = request.files["file"]
    filename = f.filename
    data = f.read()
    total = len(data)
    num_chunks = math.ceil(total / CHUNK_SIZE) if total > 0 else 1
    chunks_meta = []
    for i in range(num_chunks):
        start, end = i * CHUNK_SIZE, min(total, (i + 1) * CHUNK_SIZE)
        chunk_bytes = data[start:end]
        sha = hashlib.sha256(chunk_bytes).hexdigest()
        r = requests.post(NAMENODE_API + "/allocate_chunk", json={
            "filename": filename, "chunk_index": i, "chunk_sha256": sha
        })
        if r.status_code != 200:
            return "Namenode allocation failed", 500
        replicas = r.json().get("replicas", [])
        if not replicas:
            return "No healthy datanodes available", 503
        for rep in replicas:
            try:
                with socket.create_connection((rep["host"], rep["port"]), timeout=8) as s:
                    send_msg(s, json.dumps({
                        "cmd": "store_chunk",
                        "meta": {"filename": filename, "chunk_index": i, "sha256": sha, "size": len(chunk_bytes)}
                    }).encode())
                    send_msg(s, chunk_bytes)
                    recv_msg(s)
            except Exception as e:
                print("Error sending to datanode", rep, e)
        chunks_meta.append({"index": i, "sha256": sha, "size": len(chunk_bytes), "replicas": replicas})
    requests.post(NAMENODE_API + "/commit_file", json={"filename": filename, "chunks": chunks_meta, "filesize": total})
    return f"<h3 style='color:green;'>Uploaded {filename} ({total} bytes) in {len(chunks_meta)} chunks.</h3><a href='/'>Back</a>"

@app.route("/download/<path:filename>")
def download(filename):
    r = requests.get(NAMENODE_API + "/file_info/" + filename)
    if r.status_code != 200:
        return "File not found", 404
    info = r.json()
    assembled = io.BytesIO()
    for ch in sorted(info["chunks"], key=lambda x: x["index"]):
        got = False
        for rep in ch["replicas"]:
            try:
                with socket.create_connection((rep["host"], rep["port"]), timeout=8) as s:
                    send_msg(s, json.dumps({"cmd": "get_chunk", "meta": {"filename": filename, "chunk_index": ch["index"]}}).encode())
                    hdr = recv_msg(s)
                    if not hdr: continue
                    meta = json.loads(hdr.decode())
                    if meta.get("status") != "ok": continue
                    chunk_bytes = recv_msg(s)
                    if hashlib.sha256(chunk_bytes).hexdigest() != ch["sha256"]: continue
                    assembled.write(chunk_bytes)
                    got = True
                    break
            except Exception:
                continue
        if not got:
            return f"Failed to retrieve chunk {ch['index']}", 500
    assembled.seek(0)
    return send_file(assembled, as_attachment=True, download_name=filename)

if __name__ == "__main__":
    print(f"[Client UI] Running on http://0.0.0.0:{args.port}  (Namenode API: {NAMENODE_API})")
    app.run(host="0.0.0.0", port=args.port, threaded=True)