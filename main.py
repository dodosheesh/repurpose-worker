import os
import subprocess
import requests
import uuid
from fastapi import FastAPI, HTTPException, Header, Query
from pydantic import BaseModel
from typing import Optional, Dict, Any

app = FastAPI()
WORKER_API_KEY = os.getenv("WORKER_API_KEY", "")

# simple in-memory task store (MVP)
TASKS = {}


class ProcessRequest(BaseModel):
    input_url: str
    upload_url: str
    preset: Dict[str, Any] = {}
    export: Dict[str, Any] = {}


def require_auth(auth_header: Optional[str]):
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    token = auth_header.split(" ", 1)[1].strip()
    if token != WORKER_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid token")


@app.get("/")
def root():
    return {"ok": True, "service": "repurpose-worker"}


@app.get("/health")
def health():
    try:
        out = subprocess.check_output(
            ["ffmpeg", "-version"], stderr=subprocess.STDOUT, text=True
        )
        return {"ok": True, "ffmpeg": out.splitlines()[0]}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# =========================
# REQUIRED BY LOVABLE
# =========================
@app.get("/status")
def status(task_id: str = Query(...)):
    task = TASKS.get(task_id)
    if not task:
        return {"task_id": task_id, "status": "not_found"}

    return task


@app.post("/process")
def process(req: ProcessRequest, authorization: Optional[str] = Header(None)):
    require_auth(authorization)

    task_id = str(uuid.uuid4())
    TASKS[task_id] = {"task_id": task_id, "status": "processing"}

    in_path = "/tmp/input.mp4"
    out_path = "/tmp/output.mp4"

    try:
        # ================= DOWNLOAD =================
        r = requests.get(req.input_url, stream=True, timeout=120)
        if r.status_code >= 300:
            raise Exception(f"Download failed: {r.status_code}")

        with open(in_path, "wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                if chunk:
                    f.write(chunk)

        # ================= FFMPEG =================
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            in_path,
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "20",
            "-c:a",
            "aac",
            "-b:a",
            "128k",
            "-movflags",
            "+faststart",
            out_path,
        ]

        p = subprocess.run(cmd, capture_output=True, text=True)
        if p.returncode != 0:
            raise Exception("ffmpeg failed")

        # ================= UPLOAD =================
        with open(out_path, "rb") as f:
            headers = {"Content-Type": "video/mp4"}
            up = requests.put(req.upload_url, data=f, headers=headers, timeout=300)
            if up.status_code >= 300:
                raise Exception(f"Upload failed: {up.status_code}")

        TASKS[task_id] = {"task_id": task_id, "status": "completed"}
        return {"task_id": task_id, "status": "processing"}

    except Exception as e:
        TASKS[task_id] = {
            "task_id": task_id,
            "status": "failed",
            "error": str(e),
        }
        raise HTTPException(status_code=500, detail=str(e))
