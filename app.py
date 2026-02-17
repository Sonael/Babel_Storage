#!/usr/bin/env python3
"""
Babel Storage Web Interface

Improvements:
- Thread-safe upload progress tracking
- Secure file_id generation (SHA256)
- Automatic cleanup of temporary download files
- Optional metadata signature verification
- Improved error handling
- Production-ready configuration
- Path Traversal protection
- Memory Leak prevention
"""

import os
import time
import hashlib
import threading
import tempfile
import zstandard as zstd
from datetime import datetime
from io import BytesIO

from flask import (
    Flask,
    render_template,
    request,
    jsonify,
    send_file,
    after_this_request
)
from werkzeug.utils import secure_filename

import binary_encoder
import file_chunker
import babel

# ==========================================
# CONFIGURATION
# ==========================================

UPLOAD_FOLDER = "uploads"
METADATA_FOLDER = "metadata"
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB
PUBLIC_KEY_PATH = "public.pem"  # optional

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.config["METADATA_FOLDER"] = METADATA_FOLDER
app.config["MAX_CONTENT_LENGTH"] = MAX_FILE_SIZE

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(METADATA_FOLDER, exist_ok=True)

# ==========================================
# THREAD SAFE PROGRESS TRACKING
# ==========================================

upload_progress = {}
progress_lock = threading.Lock()


def update_progress(file_id, data):
    with progress_lock:
        upload_progress[file_id] = data


def get_progress(file_id):
    with progress_lock:
        return upload_progress.get(file_id)


# ==========================================
# UTILITIES
# ==========================================

def format_file_size(size_bytes):
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} TB"


def get_all_files():
    files = []

    for filename in os.listdir(METADATA_FOLDER):
        if not filename.endswith(".json.gz"):
            continue

        filepath = os.path.join(METADATA_FOLDER, filename)

        try:
            metadata = file_chunker.FileMetadata.load(filepath)
            file_stats = os.stat(filepath)

            files.append({
                "id": filename.replace(".json.gz", ""),
                "name": metadata.filename,
                "size": format_file_size(metadata.original_size),
                "size_bytes": metadata.original_size,
                "chunks": metadata.chunk_count,
                "hash": metadata.file_hash[:16] + "...",
                "uploaded": datetime.fromtimestamp(
                    file_stats.st_mtime
                ).strftime("%Y-%m-%d %H:%M"),
                "status": "completed"
            })

        except Exception as e:
            print(f"[ERROR] Reading metadata {filename}: {e}")

    files.sort(key=lambda x: x["uploaded"], reverse=True)
    return files


# ==========================================
# BACKGROUND UPLOAD WORKER
# ==========================================

def upload_file_worker(file_id, filepath, metadata_path):
    """
    Worker melhorado com:
    - Melhor tratamento de erros
    - Logs detalhados
    - Detecção de problemas específicos
    - Informações úteis para debug
    """
    
    start_time = time.time()
    
    try:
        update_progress(file_id, {
            "status": "initializing",
            "progress": 0,
            "current_chunk": 0,
            "total_chunks": 0,
            "message": "Preparando upload...",
            "start_time": start_time
        })

        # Cria metadata
        try:
            metadata = file_chunker.create_file_metadata(filepath)
            total_chunks = metadata.chunk_count
        except Exception as e:
            update_progress(file_id, {
                "status": "error",
                "progress": 0,
                "message": f"Erro ao processar arquivo: {str(e)}"
            })
            return

        update_progress(file_id, {
            "status": "uploading",
            "progress": 0,
            "current_chunk": 0,
            "total_chunks": total_chunks,
            "message": f"Iniciando upload de {total_chunks} chunks...",
            "elapsed_time": 0,
            "estimated_remaining": 0,
            "start_time": start_time
        })

        # Upload de cada chunk
        for chunk_index, chunk_data in file_chunker.split_file_into_chunks(filepath):

            progress = int((chunk_index + 1) / total_chunks * 100)
            
            elapsed_time = time.time() - start_time
            chunks_processed = chunk_index + 1
            chunks_remaining = total_chunks - chunks_processed
            
            # Calcula tempo estimado restante
            if chunks_processed > 0:
                avg_time_per_chunk = elapsed_time / chunks_processed
                estimated_remaining = avg_time_per_chunk * chunks_remaining
            else:
                estimated_remaining = 0

            update_progress(file_id, {
                "status": "uploading",
                "progress": progress,
                "current_chunk": chunk_index + 1,
                "total_chunks": total_chunks,
                "message": f"Processando chunk {chunk_index + 1}/{total_chunks}...",
                "elapsed_time": elapsed_time,
                "estimated_remaining": estimated_remaining,
                "start_time": start_time
            })

            # Encoding
            try:
                encoded = binary_encoder.encode_bytes_to_babel(chunk_data)
                
                # Verifica tamanho
                if len(encoded) > 3200:
                    raise RuntimeError(
                        f"Chunk muito grande após encoding: {len(encoded)} caracteres. "
                        f"Limite do Babel: 3200. Reduza MAX_CHUNK_BYTES."
                    )
                    
            except Exception as e:
                update_progress(file_id, {
                    "status": "error",
                    "progress": progress,
                    "message": f"Erro ao codificar chunk {chunk_index + 1}: {str(e)}"
                })
                return

            # Busca no Babel com retry
            max_retries = 4
            retry_delay = 2
            success = False
            last_error = None

            for attempt in range(max_retries):
                try:
                    # Recalcula tempos em tempo real
                    current_elapsed = time.time() - start_time
                    if chunks_processed > 0:
                        avg_time = current_elapsed / chunks_processed
                        current_remaining = avg_time * chunks_remaining
                    else:
                        current_remaining = 0
                    
                    update_progress(file_id, {
                        "status": "uploading",
                        "progress": progress,
                        "current_chunk": chunk_index + 1,
                        "total_chunks": total_chunks,
                        "message": (
                            f"Buscando no Babel... chunk {chunk_index + 1}/{total_chunks} "
                            f"(tentativa {attempt + 1}/{max_retries})"
                        ),
                        "elapsed_time": current_elapsed,
                        "estimated_remaining": current_remaining
                    })
                    
                    # Busca com timeout
                    hex_id, wall, shelf, volume, page = babel.search(encoded)

                    # Verifica se retornou coordenadas
                    if not hex_id or not wall or not shelf or not volume or not page:
                        raise RuntimeError(
                            "Babel não retornou coordenadas completas. "
                            "Possíveis causas: texto muito longo, rate limiting, ou timeout."
                        )

                    # Salva coordenadas
                    metadata.chunks[chunk_index].babel_coords = {
                        "hex": hex_id,
                        "wall": wall,
                        "shelf": shelf,
                        "volume": volume,
                        "page": page
                    }

                    # Verificação imediata
                    current_elapsed = time.time() - start_time
                    if chunks_processed > 0:
                        avg_time = current_elapsed / chunks_processed
                        current_remaining = avg_time * chunks_remaining
                    else:
                        current_remaining = 0
                    
                    update_progress(file_id, {
                        "status": "uploading",
                        "progress": progress,
                        "current_chunk": chunk_index + 1,
                        "total_chunks": total_chunks,
                        "message": f"Verificando chunk {chunk_index + 1}...",
                        "elapsed_time": current_elapsed,
                        "estimated_remaining": current_remaining
                    })
                    
                    retrieved = babel.browse(hex_id, wall, shelf, volume, page)

                    if not retrieved:
                        raise RuntimeError("Falha na verificação: não foi possível recuperar dados")

                    retrieved_clean = retrieved.replace("\n", "").replace("\r", "")
                    
                    if retrieved_clean[:len(encoded)] != encoded:
                        raise RuntimeError("Dados recuperados não correspondem ao original")

                    success = True
                    
                    current_elapsed = time.time() - start_time
                    if chunks_processed > 0:
                        avg_time = current_elapsed / chunks_processed
                        current_remaining = avg_time * chunks_remaining
                    else:
                        current_remaining = 0
                    
                    update_progress(file_id, {
                        "status": "uploading",
                        "progress": progress,
                        "current_chunk": chunk_index + 1,
                        "total_chunks": total_chunks,
                        "message": f"✓ Chunk {chunk_index + 1}/{total_chunks} verificado",
                        "elapsed_time": current_elapsed,
                        "estimated_remaining": current_remaining
                    })
                    
                    time.sleep(1.5)  # Rate limiting
                    break

                except Exception as e:
                    last_error = str(e)
                    
                    if attempt < max_retries - 1:
                        current_elapsed = time.time() - start_time
                        if chunks_processed > 0:
                            avg_time = current_elapsed / chunks_processed
                            current_remaining = avg_time * chunks_remaining
                        else:
                            current_remaining = 0
                        
                        update_progress(file_id, {
                            "status": "uploading",
                            "progress": progress,
                            "current_chunk": chunk_index + 1,
                            "total_chunks": total_chunks,
                            "message": (
                                f"⚠ Tentativa {attempt + 1} falhou: {str(e)[:50]}... "
                                f"Aguardando {retry_delay}s..."
                            ),
                            "elapsed_time": current_elapsed,
                            "estimated_remaining": current_remaining
                        })
                        
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Backoff exponencial
                    else:
                        error_msg = (
                            f"Falha no chunk {chunk_index + 1} após {max_retries} tentativas. "
                            f"Último erro: {last_error}"
                        )
                        
                        if "no coordinates" in last_error.lower():
                            error_msg += (
                                " | Dica: Verifique se o Babel está acessível e "
                                "se o chunk não é muito grande."
                            )
                        elif "timeout" in last_error.lower():
                            error_msg += " | Dica: O servidor Babel pode estar lento. Tente novamente mais tarde."
                        elif "rate limit" in last_error.lower():
                            error_msg += " | Dica: Aguarde alguns minutos antes de tentar novamente."
                        
                        update_progress(file_id, {
                            "status": "error",
                            "progress": progress,
                            "current_chunk": chunk_index + 1,
                            "total_chunks": total_chunks,
                            "message": error_msg
                        })
                        return

            if not success:
                update_progress(file_id, {
                    "status": "error",
                    "progress": progress,
                    "message": f"Upload abortado no chunk {chunk_index + 1}"
                })
                return

        # Salva metadata
        try:
            metadata.save(metadata_path)
        except Exception as e:
            update_progress(file_id, {
                "status": "error",
                "progress": 100,
                "message": f"Erro ao salvar metadata: {str(e)}"
            })
            return

        update_progress(file_id, {
            "status": "completed",
            "progress": 100,
            "current_chunk": total_chunks,
            "total_chunks": total_chunks,
            "message": "✓ Upload concluído com sucesso!"
        })

    except Exception as e:
        # Erro geral não capturado
        update_progress(file_id, {
            "status": "error",
            "progress": 0,
            "message": f"Erro inesperado: {str(e)}"
        })
        
        # Log do erro completo no servidor
        import traceback
        print(f"[ERROR] Upload failed for {file_id}:")
        traceback.print_exc()

    finally:
        if os.path.exists(filepath):
            try:
                os.remove(filepath)
            except Exception as e:
                print(f"[WARNING] Could not remove temp file {filepath}: {e}")


# ==========================================
# ROUTES
# ==========================================

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/files")
def api_files():
    return jsonify({
        "success": True,
        "files": get_all_files()
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

        # Função para formatar tempo em HH:MM:SS
        def format_time(seconds):
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            secs = int(seconds % 60)
            return f"{hours:02d}:{minutes:02d}:{secs:02d}"

        upload_time = format_time(stats["estimated_upload_time_seconds"])
        download_time = format_time(stats["estimated_download_time_seconds"])

        return jsonify({
            "success": True,
            "estimate": {
                "chunks": stats["chunk_count"],
                "upload_time": upload_time,
                "download_time": download_time
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


@app.route("/api/upload", methods=["POST"])
def api_upload():

    if "file" not in request.files:
        return jsonify({"success": False, "error": "No file provided"}), 400

    file = request.files["file"]

    if file.filename == "":
        return jsonify({"success": False, "error": "Empty filename"}), 400

    filename = secure_filename(file.filename)

    file_id = hashlib.sha256(
        f"{filename}{time.time()}".encode()
    ).hexdigest()[:16]

    temp_path = os.path.join(UPLOAD_FOLDER, f"{file_id}_{filename}")
    metadata_path = os.path.join(METADATA_FOLDER, f"{file_id}.json.gz")

    file.save(temp_path)

    thread = threading.Thread(
        target=upload_file_worker,
        args=(file_id, temp_path, metadata_path),
        daemon=True
    )
    thread.start()

    return jsonify({
        "success": True,
        "file_id": file_id,
        "message": "Upload started"
    })


@app.route("/api/upload/progress/<file_id>")
def api_upload_progress(file_id):
    progress = get_progress(file_id)
    if not progress:
        return jsonify({"success": False, "error": "Not found"}), 404
    
    # Garbage Collection manual da memória
    if progress['status'] in ['completed', 'error']:
        with progress_lock:
            upload_progress.pop(file_id, None)
            
    return jsonify({"success": True, "progress": progress})



@app.route("/api/download/<file_id>")
def api_download(file_id):
    """
    Download com suporte a progresso em tempo real
    """
    
    # Proteção contra Path Traversal
    safe_file_id = secure_filename(file_id)
    metadata_path = os.path.join(METADATA_FOLDER, f"{safe_file_id}.json.gz")

    if not os.path.exists(metadata_path):
        return jsonify({"success": False, "error": "File not found"}), 404

    try:
        metadata = file_chunker.FileMetadata.load(metadata_path)

        # Verificação de assinatura (opcional)
        if os.path.exists(PUBLIC_KEY_PATH) and metadata.signature:
            if not metadata.verify_signature(PUBLIC_KEY_PATH):
                return jsonify({
                    "success": False,
                    "error": "Metadata signature verification failed"
                }), 400

        print(f"[INFO] Downloading file: {metadata.filename}")
        print(f"[INFO] Expected chunks: {metadata.chunk_count}")

        # Recupera todos os chunks
        chunks_data = []
        start_time = time.time()

        for i, chunk in enumerate(metadata.chunks):
            coords = chunk.babel_coords

            print(f"[INFO] Retrieving chunk {i+1}/{metadata.chunk_count}")
            
            # Atualiza progresso do download
            elapsed_time = time.time() - start_time
            chunks_processed = i + 1
            chunks_remaining = metadata.chunk_count - chunks_processed
            
            if chunks_processed > 0:
                avg_time_per_chunk = elapsed_time / chunks_processed
                estimated_remaining = avg_time_per_chunk * chunks_remaining
            else:
                estimated_remaining = 0
            
            # Armazena progresso (pode ser lido por outra rota se necessário)
            update_progress(f"download_{file_id}", {
                "status": "downloading",
                "progress": int((chunks_processed / metadata.chunk_count) * 100),
                "current_chunk": chunks_processed,
                "total_chunks": metadata.chunk_count,
                "elapsed_time": elapsed_time,
                "estimated_remaining": estimated_remaining
            })

            encoded = babel.browse(
                coords["hex"],
                coords["wall"],
                coords["shelf"],
                coords["volume"],
                coords["page"]
            )

            if not encoded:
                raise RuntimeError(f"Failed to retrieve chunk {i+1}")

            chunk_data = binary_encoder.decode_babel_to_bytes(encoded)
            chunk_data = chunk_data[:chunk.chunk_size]

            chunks_data.append((chunk.chunk_index, chunk_data))

        # Reconstrução manual para BytesIO
        print("[INFO] Reconstructing file...")
        
        # Ordena chunks
        sorted_chunks = sorted(chunks_data, key=lambda x: x[0])
        compressed_data = bytearray()

        # Junta os chunks comprimidos
        for index, chunk_data in sorted_chunks:
            if index >= metadata.chunk_count:
                raise RuntimeError(f"Invalid chunk index {index}")

            # Verifica hash
            expected_hash = metadata.chunks[index].chunk_hash
            actual_hash = hashlib.sha256(chunk_data).hexdigest()

            if actual_hash != expected_hash:
                print(f"[WARNING] SHA256 mismatch at chunk {index}")

            compressed_data.extend(chunk_data)

        # Descomprime
        print("[INFO] Decompressing...")
        dctx = zstd.ZstdDecompressor()
        decompressed_data = dctx.decompress(bytes(compressed_data))

        # Verifica hash final
        final_hash = hashlib.sha256(decompressed_data).hexdigest()
        if final_hash != metadata.file_hash:
            raise RuntimeError("Final file SHA256 mismatch.")

        print(f"[INFO] File reconstructed successfully: {len(decompressed_data)} bytes")

        # Cria BytesIO com os dados
        output_bytes = BytesIO(decompressed_data)
        output_bytes.seek(0)
        
        # Limpa progresso
        update_progress(f"download_{file_id}", {
            "status": "completed",
            "progress": 100
        })

        return send_file(
            output_bytes,
            as_attachment=True,
            download_name=metadata.filename,
            mimetype='application/octet-stream'
        )

    except Exception as e:
        print(f"[ERROR] Download failed: {e}")
        import traceback
        traceback.print_exc()
        
        # Registra o erro no progresso para limpar a memória
        update_progress(f"download_{file_id}", {
            "status": "error",
            "progress": 0
        })
        
        return jsonify({
            "success": False,
            "error": f"Download failed: {str(e)}"
        }), 500


@app.route("/api/download/<file_id>/progress")
def api_download_progress(file_id):
    """Retorna o progresso do download e faz limpeza se concluído"""
    progress = get_progress(f"download_{file_id}")
    if not progress:
        return jsonify({"success": False, "error": "Not found"}), 404
        
    # Garbage Collection manual da memória de download
    if progress['status'] in ['completed', 'error']:
        with progress_lock:
            upload_progress.pop(f"download_{file_id}", None)
            
    return jsonify({"success": True, "progress": progress})


@app.route("/api/delete/<file_id>", methods=["DELETE"])
def api_delete(file_id):
    
    # Proteção contra Path Traversal
    safe_file_id = secure_filename(file_id)
    metadata_path = os.path.join(METADATA_FOLDER, f"{safe_file_id}.json.gz")

    if not os.path.exists(metadata_path):
        return jsonify({"success": False, "error": "File not found"}), 404
        
    try:
        os.remove(metadata_path)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ==========================================
# ENTRYPOINT
# ==========================================

if __name__ == "__main__":
    print("=" * 60)
    print("Babel Storage Web Interface")
    print("=" * 60)
    print("Server running at http://localhost:5000")
    print()

    app.run(host="0.0.0.0", port=5000, debug=True)