import os
import subprocess
import requests
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
from typing import Optional, Dict, Any

app = FastAPI()
WORKER_API_KEY = os.getenv("WORKER_API_KEY", "")


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


@app.get("/health")
def health():
    try:
        out = subprocess.check_output(
            ["ffmpeg", "-version"], stderr=subprocess.STDOUT, text=True
        )
        return {"ok": True, "ffmpeg": out.splitlines()[0]}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/process")
def process(req: ProcessRequest, authorization: Optional[str] = Header(None)):
    require_auth(authorization)

    in_path = "/tmp/input.mp4"
    out_path = "/tmp/output.mp4"

    # =========================
    # DOWNLOAD INPUT
    # =========================
    try:
        r = requests.get(req.input_url, stream=True, timeout=120)
        if r.status_code >= 300:
            raise HTTPException(
                status_code=502,
                detail=f"Download failed: {r.status_code} {r.text[:200]}",
            )

        with open(in_path, "wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                if chunk:
                    f.write(chunk)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"Download error: {str(e)[:200]}",
        )

    # =========================
    # FFMPEG PROCESS
    # =========================
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
        raise HTTPException(
            status_code=500,
            detail=f"ffmpeg failed: {p.stderr[-400:]}",
        )

    # =========================
    # UPLOAD OUTPUT
    # =========================
    try:
        with open(out_path, "rb") as f:
            headers = {"Content-Type": "video/mp4"}
            up = requests.put(
                req.upload_url,
                data=f,
                headers=headers,
                timeout=300,
            )

            if up.status_code >= 300:
                raise HTTPException(
                    status_code=502,
                    detail=f"Upload failed: {up.status_code} {up.text[:200]}",
                )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"Upload error: {str(e)[:200]}",
        )

    return {"ok": True}
