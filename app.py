#!/usr/bin/env python3
"""
Babel Storage Web Interface

Feature parity with the CLI (babel_storage.py):
- Upload with optional RSA metadata signing        (BSP v4, `upload --privkey`)
- Download with strict mode and signature check    (BSP v5, `download --strict --pubkey`)
- Offline metadata verification                    (`verify-metadata`)
- Full metadata / chunk coordinate inspection      (`info`)
- Metadata export and import (the .json.gz artifact the CLI produces)

Both front-ends share the same engine (babel_storage.BabelStorage), so
verification behaviour cannot drift between them.

Also:
- Thread-safe job tracking for uploads and downloads
- Secure file_id generation (SHA256)
- Path traversal protection
- Bounded in-memory job registry (TTL sweep)
"""

import os
import json
import time
import hashlib
import threading
from datetime import datetime
from io import BytesIO

from flask import (
    Flask,
    render_template,
    request,
    jsonify,
    send_file
)
from werkzeug.utils import secure_filename

import file_chunker
import babel
import babel_storage
import crypto_utils

# ==========================================
# CONFIGURATION
# ==========================================

UPLOAD_FOLDER = "uploads"
METADATA_FOLDER = "metadata"

# ---- Startup-only knobs (env/CLI, not editable from the web UI) ----
# Caminhos de chave e bind do servidor são sensíveis à segurança / exigem
# reinício, então ficam fora do painel editável.
PRIVATE_KEY_PATH = os.environ.get("BABEL_PRIVATE_KEY", "private.pem")
PUBLIC_KEY_PATH = os.environ.get("BABEL_PUBLIC_KEY", "public.pem")

# Onde as configurações editáveis são persistidas entre reinícios.
CONFIG_FILE = os.environ.get("BABEL_CONFIG_FILE", "babel_config.json")

# Jobs concluídos são descartados após este intervalo.
JOB_TTL_SECONDS = 15 * 60

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.config["METADATA_FOLDER"] = METADATA_FOLDER

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(METADATA_FOLDER, exist_ok=True)

# ==========================================
# RUNTIME SETTINGS (editable via UI + CLI-parity env vars, persisted)
# ==========================================
#
# Precedência na inicialização: padrões embutidos → variáveis de ambiente →
# arquivo persistido (babel_config.json). O arquivo representa a última escolha
# explícita feita pela interface e vence. Edições pela UI atualizam o arquivo.

def _env_num(name, default, cast):
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return cast(raw)
    except (TypeError, ValueError):
        print(f"[WARNING] {name} inválido ({raw!r}); usando {default}")
        return default


def _env_bool(name, default):
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() not in ("0", "false", "no", "off", "")


# Limites de validação [min, max] para cada configuração editável.
SETTING_BOUNDS = {
    "rate_limit_delay": (0.0, 60.0),
    "max_file_size": (1 * 1024 * 1024, 2 * 1024 * 1024 * 1024),
    "max_retries": (1, 10),
    "retry_delay": (0.0, 30.0),
}

DEFAULT_SETTINGS = {
    "rate_limit_delay": _env_num("BABEL_RATE_LIMIT", babel_storage.RATE_LIMIT_DELAY, float),
    "max_file_size": _env_num("BABEL_MAX_FILE_SIZE", 100 * 1024 * 1024, int),
    "default_strict": _env_bool("BABEL_STRICT", True),
    "max_retries": _env_num("BABEL_MAX_RETRIES", babel_storage.MAX_CHUNK_RETRIES, int),
    "retry_delay": _env_num("BABEL_RETRY_DELAY", babel_storage.INITIAL_RETRY_DELAY, float),
}

SETTINGS = dict(DEFAULT_SETTINGS)
settings_lock = threading.Lock()


def clamp_setting(key, value):
    """Coerce and clamp one setting to its bounds; returns (ok, value_or_error)."""
    if key == "default_strict":
        return True, bool(value)

    if key not in SETTING_BOUNDS:
        return False, f"Unknown setting: {key}"

    lo, hi = SETTING_BOUNDS[key]
    caster = int if isinstance(lo, int) else float
    try:
        num = caster(value)
    except (TypeError, ValueError):
        return False, f"{key} must be a number"

    if not (lo <= num <= hi):
        return False, f"{key} must be between {lo} and {hi}"

    return True, num


def get_settings():
    with settings_lock:
        return dict(SETTINGS)


def apply_settings_side_effects():
    """Reflect settings that other subsystems read from elsewhere."""
    with settings_lock:
        app.config["MAX_CONTENT_LENGTH"] = int(SETTINGS["max_file_size"])


def save_settings():
    snapshot = get_settings()
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, indent=2)
    except Exception as e:
        print(f"[WARNING] Could not persist settings to {CONFIG_FILE}: {e}")


def load_settings():
    """Overlay the persisted config file on top of env/defaults, then apply."""
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, encoding="utf-8") as f:
                saved = json.load(f)
        except Exception as e:
            print(f"[WARNING] Could not read {CONFIG_FILE}: {e}")
            saved = {}

        with settings_lock:
            for key in DEFAULT_SETTINGS:
                if key in saved:
                    ok, value = clamp_setting(key, saved[key])
                    if ok:
                        SETTINGS[key] = value

    apply_settings_side_effects()


load_settings()

# ==========================================
# THREAD SAFE JOB TRACKING
# ==========================================

jobs = {}
jobs_lock = threading.Lock()


def new_job(job_id, kind, **fields):
    with jobs_lock:
        _sweep_locked()
        jobs[job_id] = {
            "id": job_id,
            "kind": kind,
            "status": "initializing",
            "progress": 0,
            "current_chunk": 0,
            "total_chunks": 0,
            "message": "Preparing...",
            "started_at": time.time(),
            "finished_at": None,
            "elapsed_time": 0,
            "estimated_remaining": 0,
            **fields
        }
    return job_id


def update_job(job_id, **fields):
    with jobs_lock:
        job = jobs.get(job_id)
        if job is None:
            return

        job.update(fields)

        elapsed = time.time() - job["started_at"]
        job["elapsed_time"] = elapsed

        done = job.get("current_chunk") or 0
        total = job.get("total_chunks") or 0

        if total:
            job["progress"] = min(int(done / total * 100), 100)

        if done and total and done <= total:
            job["estimated_remaining"] = (elapsed / done) * (total - done)

        if job["status"] in ("completed", "error"):
            job["progress"] = 100 if job["status"] == "completed" else job["progress"]
            job["estimated_remaining"] = 0
            job["finished_at"] = time.time()


def get_job(job_id):
    with jobs_lock:
        job = jobs.get(job_id)
        if job is None:
            return None
        # Nunca serializa o payload binário para o cliente.
        return {k: v for k, v in job.items() if k != "result"}


def pop_job_result(job_id):
    with jobs_lock:
        job = jobs.get(job_id)
        if job is None:
            return None
        return job.pop("result", None)


def discard_job(job_id):
    with jobs_lock:
        jobs.pop(job_id, None)


def _sweep_locked():
    """Drop finished jobs (and their payloads) past the TTL."""
    cutoff = time.time() - JOB_TTL_SECONDS
    for job_id in [
        j_id for j_id, j in jobs.items()
        if j.get("finished_at") and j["finished_at"] < cutoff
    ]:
        jobs.pop(job_id, None)


def job_progress_cb(job_id):
    """Bridge engine progress events to the job registry."""

    def _cb(event):
        fields = {
            "message": event.get("message", ""),
        }

        if event.get("total_chunks") is not None:
            fields["total_chunks"] = event["total_chunks"]
        if event.get("current_chunk") is not None:
            fields["current_chunk"] = event["current_chunk"]

        status = event.get("status")
        if status == "initializing":
            fields["status"] = "initializing"
        elif status == "running":
            fields["status"] = "running"
        # `completed` só é marcado pela worker, depois de salvar/entregar.

        update_job(job_id, **fields)

    return _cb


# ==========================================
# UTILITIES
# ==========================================

def format_file_size(size_bytes):
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} TB"


def clean_display_name(file_id, filename):
    """
    Strip a legacy "<file_id>_" prefix from a recorded filename.

    Older uploads recorded the temp filename (which carried the file_id) into
    the metadata. New uploads store the clean name, so this is a no-op for
    them. Done at display/download time only — the stored metadata (and thus
    its signature) is never touched.
    """
    if not filename:
        return filename
    prefix = f"{file_id}_"
    return filename[len(prefix):] if filename.startswith(prefix) else filename


def metadata_path_for(file_id):
    """Resolve a file_id to its metadata path, blocking path traversal."""

    safe_file_id = secure_filename(file_id)
    if not safe_file_id:
        return None, None

    path = os.path.join(METADATA_FOLDER, f"{safe_file_id}.json.gz")
    return safe_file_id, path


def load_metadata_or_404(file_id):
    """Returns (metadata, path, error_response). Exactly one of the first two
    pairs and the error is meaningful."""

    safe_file_id, path = metadata_path_for(file_id)

    if not path or not os.path.exists(path):
        return None, None, (
            jsonify({"success": False, "error": "File not found"}), 404
        )

    try:
        return file_chunker.FileMetadata.load(path), path, None
    except Exception as e:
        return None, None, (
            jsonify({
                "success": False,
                "error": f"Could not read metadata: {e}"
            }), 500
        )


def public_key_or_none():
    return PUBLIC_KEY_PATH if os.path.exists(PUBLIC_KEY_PATH) else None


def signature_status(metadata):
    """
    Whether a signed metadata still verifies against the CURRENT public key.

    Returns:
      True  — signed and verifies
      False — signed but does NOT verify (e.g. the key was regenerated)
      None  — not signed, or no public key on the server to check against
    """
    if not metadata.signature:
        return None

    public_key = public_key_or_none()
    if not public_key:
        return None

    try:
        return metadata.verify_signature(public_key)
    except Exception:
        return False


def private_key_or_none():
    return PRIVATE_KEY_PATH if os.path.exists(PRIVATE_KEY_PATH) else None


def wants_strict(default=None):
    """Read the strict flag from a JSON body or a query string."""

    if default is None:
        default = get_settings()["default_strict"]

    raw = None
    if request.is_json:
        raw = (request.get_json(silent=True) or {}).get("strict")
    if raw is None:
        raw = request.args.get("strict")
    if raw is None:
        raw = request.form.get("strict")
    if raw is None:
        return default

    return str(raw).lower() in ("1", "true", "yes", "on")


def get_all_files():
    files = []

    for filename in os.listdir(METADATA_FOLDER):
        if not filename.endswith(".json.gz"):
            continue

        filepath = os.path.join(METADATA_FOLDER, filename)

        try:
            metadata = file_chunker.FileMetadata.load(filepath)
            file_stats = os.stat(filepath)

            file_id = filename.replace(".json.gz", "")
            files.append({
                "id": file_id,
                "name": clean_display_name(file_id, metadata.filename),
                "size": format_file_size(metadata.original_size),
                "size_bytes": metadata.original_size,
                "chunks": metadata.chunk_count,
                "hash": metadata.file_hash[:16] + "...",
                "full_hash": metadata.file_hash,
                "protocol_version": metadata.protocol_version,
                "signed": bool(metadata.signature),
                "signature_valid": signature_status(metadata),
                "missing_coordinates": sum(
                    1 for c in metadata.chunks if not c.babel_coords
                ),
                "uploaded": datetime.fromtimestamp(
                    file_stats.st_mtime
                ).strftime("%Y-%m-%d %H:%M"),
                "uploaded_ts": file_stats.st_mtime,
                "status": "completed"
            })

        except Exception as e:
            print(f"[ERROR] Reading metadata {filename}: {e}")

    files.sort(key=lambda x: x["uploaded_ts"], reverse=True)
    return files


# ==========================================
# BACKGROUND WORKERS
# ==========================================

def upload_worker(file_id, filepath, metadata_path, sign, display_name):
    """Runs the CLI upload engine and mirrors its events into the job."""

    private_key = private_key_or_none() if sign else None

    s = get_settings()
    storage = babel_storage.BabelStorage(
        verbose=True,
        progress_cb=job_progress_cb(file_id),
        rate_limit_delay=s["rate_limit_delay"],
        max_retries=s["max_retries"],
        retry_delay=s["retry_delay"]
    )

    try:
        storage.upload_file(
            filepath,
            metadata_path,
            private_key_path=private_key,
            display_name=display_name
        )

        update_job(
            file_id,
            status="completed",
            signed=bool(private_key),
            message=(
                "✓ Upload completed and signed!" if private_key
                else "✓ Upload completed successfully!"
            )
        )

    except Exception as e:
        update_job(
            file_id,
            status="error",
            error=str(e),
            message=f"Upload failed: {e}"
        )

        import traceback
        print(f"[ERROR] Upload failed for {file_id}:")
        traceback.print_exc()

    finally:
        if os.path.exists(filepath):
            try:
                os.remove(filepath)
            except Exception as e:
                print(f"[WARNING] Could not remove temp file {filepath}: {e}")


def download_worker(job_id, metadata, strict, public_key):
    """Restores a file in memory so the browser can fetch it when ready."""

    s = get_settings()
    storage = babel_storage.BabelStorage(
        verbose=True,
        progress_cb=job_progress_cb(job_id),
        max_retries=s["max_retries"],
        retry_delay=s["retry_delay"]
    )

    try:
        data = storage.download_bytes(
            metadata,
            strict=strict,
            public_key_path=public_key
        )

        update_job(
            job_id,
            status="completed",
            result=data,
            result_size=len(data),
            message="✓ File restored and verified — ready to save"
        )

    except Exception as e:
        update_job(
            job_id,
            status="error",
            error=str(e),
            exit_code=babel_storage.exit_code_for(str(e)),
            message=f"Download failed: {e}"
        )

        import traceback
        print(f"[ERROR] Download failed for {job_id}:")
        traceback.print_exc()


# ==========================================
# ROUTES
# ==========================================

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/config")
def api_config():
    """Current server config: editable settings + read-only info + bounds."""

    s = get_settings()

    return jsonify({
        "success": True,
        "config": {
            "protocol_version": "v6",

            # --- editable settings (see POST /api/settings) ---
            "rate_limit_delay": s["rate_limit_delay"],
            "max_file_size": s["max_file_size"],
            "max_file_size_label": format_file_size(s["max_file_size"]),
            "default_strict": s["default_strict"],
            "max_retries": s["max_retries"],
            "retry_delay": s["retry_delay"],

            # bounds so the UI can render inputs with min/max
            "limits": {k: list(v) for k, v in SETTING_BOUNDS.items()},

            # --- read-only info ---
            "max_chunk_bytes": file_chunker.MAX_CHUNK_BYTES,
            "max_search_length": babel.MAX_SEARCH_LENGTH,
            "encoding_overhead": round(file_chunker.ENCODING_OVERHEAD, 4),
            "has_private_key": private_key_or_none() is not None,
            "has_public_key": public_key_or_none() is not None,
            "private_key_path": PRIVATE_KEY_PATH,
            "public_key_path": PUBLIC_KEY_PATH,
            "config_file": CONFIG_FILE,
        }
    })


@app.route("/api/settings", methods=["POST"])
def api_update_settings():
    """
    Update one or more editable settings, persist them, and apply live.

    Accepts any subset of: rate_limit_delay, max_file_size, default_strict,
    max_retries, retry_delay. Each value is validated and clamped to its
    bounds. Startup-only knobs (host/port/keys) are intentionally not here.
    """

    body = request.get_json(silent=True) or {}

    updates = {}
    errors = []

    for key in DEFAULT_SETTINGS:
        if key not in body:
            continue
        ok, result = clamp_setting(key, body[key])
        if ok:
            updates[key] = result
        else:
            errors.append(result)

    if errors:
        return jsonify({"success": False, "error": "; ".join(errors)}), 400

    if not updates:
        return jsonify({"success": False, "error": "No known settings provided"}), 400

    with settings_lock:
        SETTINGS.update(updates)

    apply_settings_side_effects()
    save_settings()

    return jsonify({"success": True, "updated": updates, "config": get_settings()})


@app.route("/api/keys/generate", methods=["POST"])
def api_generate_keys():
    """
    Generate an RSA-4096 key pair on the server — the same thing the README
    asks users to do from the CLI (crypto_utils.generate_keys).

    Safety:
    - The private key is written to disk on the SERVER and never returned in
      the response.
    - Overwriting an existing private key is destructive: every metadata
      already signed with it becomes unverifiable. So an existing key is only
      replaced when the caller explicitly passes {"force": true}.
    """

    body = request.get_json(silent=True) or {}
    force = bool(body.get("force"))

    if private_key_or_none() is not None and not force:
        return jsonify({
            "success": False,
            "requires_force": True,
            "error": (
                f"A private key already exists at {PRIVATE_KEY_PATH}. "
                f"Regenerating it will permanently invalidate every metadata "
                f"already signed with the current key. Resend with force=true "
                f"to overwrite."
            )
        }), 409

    try:
        crypto_utils.generate_keys(PRIVATE_KEY_PATH, PUBLIC_KEY_PATH)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

    return jsonify({
        "success": True,
        "private_key_path": PRIVATE_KEY_PATH,
        "public_key_path": PUBLIC_KEY_PATH,
        "message": "RSA-4096 key pair generated"
    })


@app.route("/api/files")
def api_files():
    return jsonify({
        "success": True,
        "files": get_all_files()
    })


@app.route("/api/files/<file_id>/info")
def api_file_info(file_id):
    """Web equivalent of `babel_storage.py info`."""

    metadata, path, error = load_metadata_or_404(file_id)
    if error:
        return error

    storage = babel_storage.BabelStorage(verbose=False)
    info = storage.metadata_info(metadata)

    info["id"] = secure_filename(file_id)
    info["filename"] = clean_display_name(info["id"], info["filename"])
    info["signature_valid"] = signature_status(metadata)
    info["metadata_size"] = os.path.getsize(path)
    info["uploaded"] = datetime.fromtimestamp(
        os.stat(path).st_mtime
    ).strftime("%Y-%m-%d %H:%M")

    return jsonify({"success": True, "info": info})


@app.route("/api/files/<file_id>/resign", methods=["POST"])
def api_file_resign(file_id):
    """
    Re-sign a metadata file with the server's CURRENT private key.

    After regenerating the key pair, metadata signed with the old key no
    longer verifies; this restores a valid signature (or signs a file that
    was never signed). Only the metadata's `sig` field changes — the
    coordinates are untouched.
    """

    metadata, path, error = load_metadata_or_404(file_id)
    if error:
        return error

    private_key = private_key_or_none()
    if not private_key:
        return jsonify({
            "success": False,
            "error": f"No private key at {PRIVATE_KEY_PATH} to sign with."
        }), 400

    try:
        metadata.sign(private_key)
        metadata.save(path)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

    return jsonify({
        "success": True,
        "signature_valid": signature_status(metadata),
        "message": "Metadata re-signed with the current key"
    })


@app.route("/api/files/<file_id>/verify", methods=["POST"])
def api_file_verify(file_id):
    """Web equivalent of `babel_storage.py verify-metadata` (offline, no network)."""

    metadata, _, error = load_metadata_or_404(file_id)
    if error:
        return error

    strict = wants_strict()

    storage = babel_storage.BabelStorage(verbose=False)

    try:
        report = storage.verify_metadata_report(
            metadata,
            public_key_path=public_key_or_none(),
            strict=strict
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

    report["filename"] = metadata.filename
    report["public_key"] = public_key_or_none()

    return jsonify({"success": True, "report": report})


@app.route("/api/files/<file_id>/verify-chunk", methods=["POST"])
def api_file_verify_chunk(file_id):
    """
    Web equivalent of `babel_storage.py verify-chunk` (BSP v6).

    Retrieves ONLY the requested chunk from Babel and checks its Merkle
    inclusion proof — the one network operation among the verify routes.
    """

    metadata, _, error = load_metadata_or_404(file_id)
    if error:
        return error

    body = request.get_json(silent=True) or {}
    index = body.get("index", request.args.get("index"))

    try:
        index = int(index)
    except (TypeError, ValueError):
        return jsonify({"success": False, "error": "A chunk index is required"}), 400

    storage = babel_storage.BabelStorage(verbose=False)

    try:
        report = storage.verify_chunk_report(
            metadata,
            index,
            public_key_path=public_key_or_none() if metadata.signature else None
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400

    return jsonify({"success": True, "report": report})


@app.route("/api/files/<file_id>/metadata")
def api_file_metadata(file_id):
    """
    Export the .json.gz artifact.

    The metadata file is the only thing that makes a file recoverable, so
    the web UI must be able to hand it back for offline backup or for use
    with the CLI.
    """

    metadata, path, error = load_metadata_or_404(file_id)
    if error:
        return error

    clean = clean_display_name(secure_filename(file_id), metadata.filename)
    download_name = f"{os.path.splitext(clean)[0]}.json.gz"

    return send_file(
        path,
        as_attachment=True,
        download_name=download_name,
        mimetype="application/gzip"
    )


@app.route("/api/metadata/import", methods=["POST"])
def api_metadata_import():
    """Import a .json.gz produced by the CLI (or exported from another instance)."""

    if "file" not in request.files:
        return jsonify({"success": False, "error": "No file provided"}), 400

    upload = request.files["file"]

    if upload.filename == "":
        return jsonify({"success": False, "error": "Empty filename"}), 400

    if not upload.filename.endswith(".json.gz"):
        return jsonify({
            "success": False,
            "error": "Expected a BabelStorage metadata file (.json.gz)"
        }), 400

    file_id = hashlib.sha256(
        f"{secure_filename(upload.filename)}{time.time()}".encode()
    ).hexdigest()[:16]

    target = os.path.join(METADATA_FOLDER, f"{file_id}.json.gz")
    upload.save(target)

    try:
        metadata = file_chunker.FileMetadata.load(target)
    except Exception as e:
        os.remove(target)
        return jsonify({
            "success": False,
            "error": f"Invalid metadata file: {e}"
        }), 400

    missing_coords = sum(1 for c in metadata.chunks if not c.babel_coords)

    return jsonify({
        "success": True,
        "file_id": file_id,
        "filename": metadata.filename,
        "chunks": metadata.chunk_count,
        "signed": bool(metadata.signature),
        "missing_coordinates": missing_coords
    })


@app.route("/api/estimate", methods=["POST"])
def api_estimate():
    if "file" not in request.files:
        return jsonify({"success": False, "error": "No file provided"}), 400

    file = request.files["file"]

    if file.filename == "":
        return jsonify({"success": False, "error": "Empty filename"}), 400

    # Salva com nome único para evitar colisões entre requisições simultâneas
    safe_name = secure_filename(file.filename)
    temp_path = os.path.join(app.config["UPLOAD_FOLDER"], f"temp_est_{time.time()}_{safe_name}")

    try:
        # Salva o arquivo temporariamente para realizar a estimativa real de compressão (zstd)
        file.save(temp_path)

        stats = file_chunker.estimate_storage_requirements(temp_path)

        upload_time = format_duration(stats["estimated_upload_time_seconds"])
        download_time = format_duration(stats["estimated_download_time_seconds"])

        return jsonify({
            "success": True,
            "estimate": {
                "chunks": stats["chunk_count"],
                "upload_time": upload_time,
                "download_time": download_time,
                "compressed_size": format_file_size(stats["compressed_size_bytes"]),
                "compression_ratio": (
                    round(stats["compressed_size_bytes"] / stats["original_size_bytes"] * 100)
                    if stats["original_size_bytes"] else 100
                )
            }
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        # Garante que o arquivo órfão nunca ficará preso no disco
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception as cleanup_error:
                print(f"[WARNING] Erro ao limpar arquivo temporário de estimativa: {cleanup_error}")


def format_duration(seconds):
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


@app.route("/api/upload", methods=["POST"])
def api_upload():

    if "file" not in request.files:
        return jsonify({"success": False, "error": "No file provided"}), 400

    file = request.files["file"]

    if file.filename == "":
        return jsonify({"success": False, "error": "Empty filename"}), 400

    filename = secure_filename(file.filename)

    # Equivalente de `upload --privkey` (BSP v4).
    sign = str(request.form.get("sign", "")).lower() in ("1", "true", "yes", "on")

    if sign and not private_key_or_none():
        return jsonify({
            "success": False,
            "error": (
                f"Signing requested but no private key found at {PRIVATE_KEY_PATH}. "
                f"Generate one with: python -c \"from crypto_utils import generate_keys; "
                f"generate_keys('{PRIVATE_KEY_PATH}', '{PUBLIC_KEY_PATH}')\""
            )
        }), 400

    file_id = hashlib.sha256(
        f"{filename}{time.time()}".encode()
    ).hexdigest()[:16]

    temp_path = os.path.join(UPLOAD_FOLDER, f"{file_id}_{filename}")
    metadata_path = os.path.join(METADATA_FOLDER, f"{file_id}.json.gz")

    file.save(temp_path)

    new_job(file_id, "upload", filename=filename, signed=sign)

    thread = threading.Thread(
        target=upload_worker,
        args=(file_id, temp_path, metadata_path, sign, filename),
        daemon=True
    )
    thread.start()

    return jsonify({
        "success": True,
        "file_id": file_id,
        "signed": sign,
        "message": "Upload started"
    })


@app.route("/api/upload/progress/<file_id>")
def api_upload_progress(file_id):
    progress = get_job(file_id)
    if not progress:
        return jsonify({"success": False, "error": "Not found"}), 404

    if progress["status"] in ("completed", "error"):
        discard_job(file_id)

    return jsonify({"success": True, "progress": progress})


@app.route("/api/download/<file_id>/start", methods=["POST"])
def api_download_start(file_id):
    """
    Start a restore in the background.

    Restoring a file takes ~1s per chunk, so it cannot happen inside the
    request that serves the bytes — the browser would sit on a dead
    connection with no progress. The job prepares the file; the client
    polls it and then fetches the result.
    """

    metadata, _, error = load_metadata_or_404(file_id)
    if error:
        return error

    strict = wants_strict()
    public_key = public_key_or_none() if metadata.signature else None

    if strict and metadata.signature and not public_key:
        return jsonify({
            "success": False,
            "error": (
                f"Strict mode: metadata is signed but no public key is available "
                f"at {PUBLIC_KEY_PATH} to verify it."
            )
        }), 400

    job_id = hashlib.sha256(
        f"download{file_id}{time.time()}".encode()
    ).hexdigest()[:16]

    clean_name = clean_display_name(secure_filename(file_id), metadata.filename)

    new_job(
        job_id,
        "download",
        file_id=secure_filename(file_id),
        filename=clean_name,
        total_chunks=metadata.chunk_count,
        strict=strict,
        verifying_signature=bool(public_key)
    )

    thread = threading.Thread(
        target=download_worker,
        args=(job_id, metadata, strict, public_key),
        daemon=True
    )
    thread.start()

    return jsonify({
        "success": True,
        "job_id": job_id,
        "filename": clean_name,
        "total_chunks": metadata.chunk_count,
        "strict": strict,
        "verifying_signature": bool(public_key)
    })


@app.route("/api/download/job/<job_id>")
def api_download_progress(job_id):
    job = get_job(job_id)

    if not job or job.get("kind") != "download":
        return jsonify({"success": False, "error": "Not found"}), 404

    # Ao contrário do upload, o job só é descartado quando os bytes forem
    # buscados (ou pelo TTL) — o payload ainda está pendurado nele.
    if job["status"] == "error":
        discard_job(job_id)

    return jsonify({"success": True, "progress": job})


@app.route("/api/download/job/<job_id>/file")
def api_download_fetch(job_id):
    job = get_job(job_id)

    if not job or job.get("kind") != "download":
        return jsonify({"success": False, "error": "Not found"}), 404

    if job["status"] != "completed":
        return jsonify({
            "success": False,
            "error": f"Download is not ready (status: {job['status']})"
        }), 409

    data = pop_job_result(job_id)
    discard_job(job_id)

    if data is None:
        return jsonify({
            "success": False,
            "error": "Result already fetched or expired"
        }), 410

    return send_file(
        BytesIO(data),
        as_attachment=True,
        download_name=job.get("filename") or "restored.bin",
        mimetype="application/octet-stream"
    )


@app.route("/api/download/<file_id>")
def api_download(file_id):
    """
    Synchronous restore, for scripts (`curl -OJ .../api/download/<id>?strict=1`).

    The browser UI uses the job-based route above instead; this one blocks
    until every chunk has been retrieved.
    """

    metadata, _, error = load_metadata_or_404(file_id)
    if error:
        return error

    strict = wants_strict()
    public_key = public_key_or_none() if metadata.signature else None

    s = get_settings()
    storage = babel_storage.BabelStorage(
        verbose=True,
        max_retries=s["max_retries"],
        retry_delay=s["retry_delay"]
    )

    try:
        data = storage.download_bytes(
            metadata,
            strict=strict,
            public_key_path=public_key
        )

        return send_file(
            BytesIO(data),
            as_attachment=True,
            download_name=clean_display_name(secure_filename(file_id), metadata.filename),
            mimetype="application/octet-stream"
        )

    except Exception as e:
        print(f"[ERROR] Download failed: {e}")
        import traceback
        traceback.print_exc()

        return jsonify({
            "success": False,
            "error": f"Download failed: {e}",
            "exit_code": babel_storage.exit_code_for(str(e))
        }), 500


@app.route("/api/delete/<file_id>", methods=["DELETE"])
def api_delete(file_id):

    _, metadata_path = metadata_path_for(file_id)

    if not metadata_path or not os.path.exists(metadata_path):
        return jsonify({"success": False, "error": "File not found"}), 404

    try:
        os.remove(metadata_path)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.errorhandler(413)
def handle_too_large(_):
    limit = get_settings()["max_file_size"]
    return jsonify({
        "success": False,
        "error": f"File exceeds the {format_file_size(limit)} limit"
    }), 413


# ==========================================
# ENTRYPOINT
# ==========================================

if __name__ == "__main__":
    host = os.environ.get("BABEL_HOST", "127.0.0.1")
    port = int(os.environ.get("BABEL_PORT", "5050"))
    # O debugger do Werkzeug executa código arbitrário: nunca ligado por padrão.
    debug = os.environ.get("BABEL_DEBUG", "0") in ("1", "true", "True")

    s = get_settings()
    print("=" * 60)
    print("Babel Storage Web Interface")
    print("=" * 60)
    print(f"Protocol      : BSP v6")
    print(f"Strict mode   : {'on' if s['default_strict'] else 'off'} (default)")
    _rl = s["rate_limit_delay"]
    print(f"Rate limit    : {_rl}s between chunks"
          f"{' (disabled)' if not _rl else ''}")
    print(f"Retries       : {s['max_retries']} per chunk, {s['retry_delay']}s initial backoff")
    print(f"Max upload    : {format_file_size(s['max_file_size'])}")
    print(f"Settings file : {CONFIG_FILE}")
    print(f"Private key   : {PRIVATE_KEY_PATH} "
          f"({'found' if private_key_or_none() else 'not found — signing disabled'})")
    print(f"Public key    : {PUBLIC_KEY_PATH} "
          f"({'found' if public_key_or_none() else 'not found — signature checks disabled'})")
    print(f"Server running at http://{host}:{port}")
    print()

    app.run(host=host, port=port, debug=debug)
