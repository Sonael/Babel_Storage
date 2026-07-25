#!/usr/bin/env python3
"""
Babel File Storage System (BSP v5)

Implements:
- BSP v1: Full file SHA256 integrity
- BSP v2: Per-chunk SHA256 integrity
- BSP v4: RSA metadata signature
- BSP v5: Strict mode + offline verification
- BSP v6: Merkle tree root + single-chunk partial verification

This module is the single storage engine used by both front-ends:
the CLI below and the web interface in app.py. Callers that need to
report progress (the web worker) pass a `progress_cb`; the CLI just
prints through `log()`.
"""

import os
import sys
import time
import argparse
import hashlib
from typing import Callable, Dict, List, Optional, Tuple

import binary_encoder
import file_chunker
import babel
import merkle


# Retry policy shared by upload and download chunk operations.
MAX_CHUNK_RETRIES = 4
INITIAL_RETRY_DELAY = 2
RATE_LIMIT_DELAY = 1.5

# Fallbacks for consoles that cannot encode the status glyphs (Windows cp1252).
_ASCII_FALLBACK = {"✓": "[OK]", "⚠": "[!]", "✗": "[X]"}


def safe_print(message: str):
    """
    print() that never aborts a transfer.

    On a non-UTF-8 console (cp1252 by default on Windows, and whenever
    output is piped) a status glyph raises UnicodeEncodeError. That must
    not take down an upload that is otherwise fine.
    """
    try:
        print(message)
    except UnicodeEncodeError:
        for glyph, replacement in _ASCII_FALLBACK.items():
            message = message.replace(glyph, replacement)
        encoding = getattr(sys.stdout, "encoding", None) or "ascii"
        print(message.encode(encoding, "replace").decode(encoding, "replace"))


class BabelStorage:
    """Main class for file storage operations."""

    def __init__(self, verbose: bool = True,
                 progress_cb: Optional[Callable[[dict], None]] = None,
                 rate_limit_delay: Optional[float] = None,
                 max_retries: Optional[int] = None,
                 retry_delay: Optional[float] = None):
        self.verbose = verbose
        self.progress_cb = progress_cb
        # Pausa entre chunks para não sobrecarregar o serviço de terceiros
        # (libraryofbabel.info). Configurável; 0 desativa a espera.
        self.rate_limit_delay = (
            RATE_LIMIT_DELAY if rate_limit_delay is None else max(0.0, rate_limit_delay)
        )
        # Política de retry por chunk (upload e download).
        self.max_retries = (
            MAX_CHUNK_RETRIES if max_retries is None else max(1, int(max_retries))
        )
        self.retry_delay = (
            INITIAL_RETRY_DELAY if retry_delay is None else max(0.0, retry_delay)
        )

    def log(self, message: str, level: str = "INFO"):
        if self.verbose:
            safe_print(f"[{level}] {message}")

    def emit(self, **event):
        """
        Publish a structured progress event.

        Never lets a misbehaving observer abort a transfer.
        """
        if not self.progress_cb:
            return

        try:
            self.progress_cb(event)
        except Exception as e:
            self.log(f"Progress callback failed: {e}", "WARNING")

    # ============================================================
    # UPLOAD
    # ============================================================

    def upload_file(self, filepath: str,
                    metadata_output: str = None,
                    private_key_path: str = None,
                    resume: bool = True,
                    display_name: str = None):

        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File not found: {filepath}")

        self.log("=" * 60)
        self.log(f"UPLOADING: {filepath}")
        self.log("=" * 60)

        self.emit(
            stage="upload",
            status="initializing",
            message="Preparing upload..."
        )

        # `display_name` records the original filename regardless of the temp
        # path the file is read from (the web saves under a unique name).
        metadata = file_chunker.create_file_metadata(filepath, filename=display_name)

        # Retomada: se já existe uma metadata parcial do MESMO arquivo,
        # reaproveita as coordenadas já encontradas. A própria metadata é
        # o arquivo de progresso (RFC-0006 §1.4).
        resumed = self._resume_from(metadata, metadata_output) if resume else 0

        self.log(f"File: {metadata.filename}")
        self.log(f"Size: {metadata.original_size:,} bytes")
        self.log(f"SHA256: {metadata.file_hash}")
        self.log(f"Chunks: {metadata.chunk_count}")
        if resumed:
            self.log(f"Resuming: {resumed}/{metadata.chunk_count} chunks already uploaded")

        self.emit(
            stage="upload",
            status="running",
            current_chunk=resumed,
            total_chunks=metadata.chunk_count,
            resumed=resumed,
            message=(
                f"Resuming — {resumed}/{metadata.chunk_count} chunks already done..."
                if resumed else
                f"Starting upload of {metadata.chunk_count} chunks..."
            )
        )

        start_time = time.time()

        for chunk_index, chunk_data in file_chunker.split_file_into_chunks(filepath):

            # Pula chunks já enviados numa execução anterior.
            if metadata.chunks[chunk_index].babel_coords:
                self.emit(
                    stage="upload",
                    status="running",
                    current_chunk=chunk_index + 1,
                    total_chunks=metadata.chunk_count,
                    message=f"Chunk {chunk_index + 1}/{metadata.chunk_count} already uploaded — skipping"
                )
                continue

            self.log(f"\nChunk {chunk_index + 1}/{metadata.chunk_count}")

            self.emit(
                stage="upload",
                status="running",
                current_chunk=chunk_index + 1,
                total_chunks=metadata.chunk_count,
                message=f"Encoding chunk {chunk_index + 1}/{metadata.chunk_count}..."
            )

            encoded = binary_encoder.encode_bytes_to_babel(chunk_data)

            if len(encoded) > babel.MAX_SEARCH_LENGTH:
                raise RuntimeError(
                    f"Chunk too large after encoding: {len(encoded)} characters. "
                    f"Babel limit: {babel.MAX_SEARCH_LENGTH}. "
                    f"Reduce MAX_CHUNK_BYTES."
                )

            retry_delay = self.retry_delay
            success = False

            for attempt in range(self.max_retries):
                try:
                    self.emit(
                        stage="upload",
                        status="running",
                        current_chunk=chunk_index + 1,
                        total_chunks=metadata.chunk_count,
                        attempt=attempt + 1,
                        max_attempts=self.max_retries,
                        message=(
                            f"Searching Babel... chunk {chunk_index + 1}/"
                            f"{metadata.chunk_count} (attempt {attempt + 1}/{self.max_retries})"
                        )
                    )

                    hex_id, wall, shelf, volume, page = babel.search(encoded)

                    if not hex_id:
                        raise RuntimeError("Babel returned no coordinates")

                    metadata.chunks[chunk_index].babel_coords = {
                        "hex": hex_id,
                        "wall": wall,
                        "shelf": shelf,
                        "volume": volume,
                        "page": page
                    }

                    # Verificação imediata após upload (BSP v5)
                    self.emit(
                        stage="upload",
                        status="running",
                        current_chunk=chunk_index + 1,
                        total_chunks=metadata.chunk_count,
                        message=f"Verifying chunk {chunk_index + 1}..."
                    )

                    retrieved = babel.browse(hex_id, wall, shelf, volume, page)

                    if not retrieved:
                        raise RuntimeError("Verification browse failed")

                    retrieved_clean = retrieved.replace("\n", "").replace("\r", "")
                    if retrieved_clean[:len(encoded)] != encoded:
                        raise RuntimeError("Encoded data mismatch after upload")

                    self.log("✓ Verified successfully")
                    self.emit(
                        stage="upload",
                        status="running",
                        current_chunk=chunk_index + 1,
                        total_chunks=metadata.chunk_count,
                        message=(
                            f"✓ Chunk {chunk_index + 1}/{metadata.chunk_count} verified"
                        )
                    )

                    success = True
                    if self.rate_limit_delay:
                        time.sleep(self.rate_limit_delay)
                    break

                except Exception as e:
                    if attempt < self.max_retries - 1:
                        self.log(f"Attempt {attempt+1} failed: {e}", "WARNING")
                        self.emit(
                            stage="upload",
                            status="running",
                            current_chunk=chunk_index + 1,
                            total_chunks=metadata.chunk_count,
                            attempt=attempt + 1,
                            max_attempts=self.max_retries,
                            message=(
                                f"⚠ Attempt {attempt + 1} failed: {str(e)[:60]} — "
                                f"retrying in {retry_delay}s..."
                            )
                        )
                        time.sleep(retry_delay)
                        retry_delay *= 2
                    else:
                        raise RuntimeError(
                            f"Upload failed: {_explain(e, chunk_index, self.max_retries)}"
                        )

            if not success:
                raise RuntimeError("Upload aborted due to repeated failures.")

            # Persiste o progresso após cada chunk: se o upload for
            # interrompido, uma nova execução retoma daqui (RFC-0006 §1.4).
            if metadata_output:
                metadata.save(metadata_output)

        total_time = time.time() - start_time
        self.log(f"\nUpload completed in {total_time:.2f}s")

        # Assina a metadata (BSP v4)
        if private_key_path:
            self.log("Signing metadata (RSA)...")
            self.emit(
                stage="upload",
                status="running",
                current_chunk=metadata.chunk_count,
                total_chunks=metadata.chunk_count,
                message="Signing metadata (RSA-PSS)..."
            )
            metadata.sign(private_key_path)

        if metadata_output:
            metadata.save(metadata_output)
            self.log(f"Metadata saved to {metadata_output}")

        self.emit(
            stage="upload",
            status="completed",
            current_chunk=metadata.chunk_count,
            total_chunks=metadata.chunk_count,
            signed=bool(private_key_path),
            message="✓ Upload completed successfully!"
        )

        return metadata

    def _resume_from(self, metadata: file_chunker.FileMetadata,
                     metadata_output: str) -> int:
        """
        Carry over coordinates from a prior partial upload of the SAME file.

        Returns how many chunks were resumed. Safe by construction: a chunk
        is only reused when both the whole-file hash and that chunk's hash
        match, so a changed file (or a mismatched metadata) starts fresh.
        """
        if not metadata_output:
            return 0

        # Espelha a normalização de sufixo de FileMetadata.save().
        resolved = (
            metadata_output if metadata_output.endswith(".gz")
            else metadata_output + ".gz"
        )
        if not os.path.exists(resolved):
            return 0

        try:
            prior = file_chunker.FileMetadata.load(resolved)
        except Exception as e:
            self.log(f"Ignoring unreadable partial metadata: {e}", "WARNING")
            return 0

        if (prior.file_hash != metadata.file_hash
                or prior.chunk_count != metadata.chunk_count):
            # Metadata anterior é de outro arquivo; recomeça do zero.
            return 0

        resumed = 0
        for i, prior_chunk in enumerate(prior.chunks):
            if (prior_chunk.babel_coords
                    and prior_chunk.chunk_hash == metadata.chunks[i].chunk_hash):
                metadata.chunks[i].babel_coords = prior_chunk.babel_coords
                resumed += 1

        return resumed

    # ============================================================
    # DOWNLOAD
    # ============================================================

    def retrieve_chunks(self, metadata: file_chunker.FileMetadata,
                        strict: bool = False) -> List[Tuple[int, bytes]]:
        """
        Fetch and verify every chunk listed in the metadata.

        Applies the same retry/backoff policy as the upload path, so a
        transient Babel failure no longer aborts a whole restore.
        """

        chunks_data: List[Tuple[int, bytes]] = []

        for chunk in metadata.chunks:

            coords = chunk.babel_coords
            if not coords:
                raise RuntimeError(f"Missing coordinates for chunk {chunk.chunk_index}")

            self.log(f"\nRetrieving chunk {chunk.chunk_index + 1}")

            retry_delay = self.retry_delay
            chunk_data = None

            for attempt in range(self.max_retries):
                try:
                    self.emit(
                        stage="download",
                        status="running",
                        current_chunk=chunk.chunk_index + 1,
                        total_chunks=metadata.chunk_count,
                        attempt=attempt + 1,
                        max_attempts=self.max_retries,
                        message=(
                            f"Retrieving chunk {chunk.chunk_index + 1}/"
                            f"{metadata.chunk_count} (attempt {attempt + 1}/{self.max_retries})"
                        )
                    )

                    encoded = babel.browse(
                        coords["hex"],
                        coords["wall"],
                        coords["shelf"],
                        coords["volume"],
                        coords["page"]
                    )

                    if not encoded:
                        raise RuntimeError("Failed to retrieve chunk from Babel")

                    decoded = binary_encoder.decode_babel_to_bytes(encoded)
                    chunk_data = decoded[:chunk.chunk_size]
                    break

                except Exception as e:
                    if attempt < self.max_retries - 1:
                        self.log(f"Attempt {attempt+1} failed: {e}", "WARNING")
                        self.emit(
                            stage="download",
                            status="running",
                            current_chunk=chunk.chunk_index + 1,
                            total_chunks=metadata.chunk_count,
                            attempt=attempt + 1,
                            max_attempts=self.max_retries,
                            message=(
                                f"⚠ Attempt {attempt + 1} failed: {str(e)[:60]} — "
                                f"retrying in {retry_delay}s..."
                            )
                        )
                        time.sleep(retry_delay)
                        retry_delay *= 2
                    else:
                        raise RuntimeError(
                            f"Download failed: {_explain(e, chunk.chunk_index, self.max_retries)}"
                        )

            # Verificação de integridade do chunk (BSP v2)
            computed_hash = hashlib.sha256(chunk_data).hexdigest()

            if computed_hash != chunk.chunk_hash:
                if strict:
                    raise RuntimeError(
                        f"Chunk SHA256 mismatch at index {chunk.chunk_index}"
                    )
                else:
                    self.log(
                        f"WARNING: Chunk {chunk.chunk_index} hash mismatch",
                        "WARNING"
                    )
                    self.emit(
                        stage="download",
                        status="running",
                        current_chunk=chunk.chunk_index + 1,
                        total_chunks=metadata.chunk_count,
                        warning=True,
                        message=(
                            f"⚠ Chunk {chunk.chunk_index + 1} SHA256 mismatch "
                            f"(strict mode off, continuing)"
                        )
                    )
            else:
                self.log("✓ Chunk SHA256 verified")
                self.emit(
                    stage="download",
                    status="running",
                    current_chunk=chunk.chunk_index + 1,
                    total_chunks=metadata.chunk_count,
                    message=(
                        f"✓ Chunk {chunk.chunk_index + 1}/{metadata.chunk_count} verified"
                    )
                )

            chunks_data.append((chunk.chunk_index, chunk_data))

        if len(chunks_data) != metadata.chunk_count:
            raise RuntimeError("Chunk count mismatch during download")

        return chunks_data

    def download_bytes(self,
        metadata: file_chunker.FileMetadata,
        strict: bool = False,
        public_key_path: str = None) -> bytes:
        """
        Restore a file fully in memory. Used by the web interface, which
        streams the result to the browser instead of writing it to disk.
        """

        # Verificação de assinatura (BSP v4)
        if public_key_path:
            self.log("Verifying metadata signature...")
            self.emit(
                stage="download",
                status="running",
                message="Verifying metadata signature..."
            )
            if not metadata.verify_signature(public_key_path):
                raise RuntimeError("Invalid metadata digital signature.")
            self.log("✓ Signature verified")

        # Verificação da raiz Merkle (BSP v6). É offline e barata: garante
        # que a lista de hashes que vamos usar bate com a raiz assinada
        # antes de gastar minutos recuperando chunks.
        if metadata.merkle_root and not metadata.verify_merkle_root():
            msg = "Merkle root mismatch — the chunk-hash list was altered"
            if strict:
                raise RuntimeError(msg)
            self.log(f"WARNING: {msg}", "WARNING")

        self.log(f"File: {metadata.filename}")
        self.log(f"Expected SHA256: {metadata.file_hash}")
        self.log(f"Chunks: {metadata.chunk_count}")

        chunks_data = self.retrieve_chunks(metadata, strict=strict)

        self.log("\nReconstructing file...")
        self.emit(
            stage="download",
            status="running",
            current_chunk=metadata.chunk_count,
            total_chunks=metadata.chunk_count,
            message="Reconstructing file..."
        )

        data = file_chunker.reconstruct_bytes_from_chunks(
            chunks_data,
            metadata,
            strict=strict
        )

        self.emit(
            stage="download",
            status="completed",
            current_chunk=metadata.chunk_count,
            total_chunks=metadata.chunk_count,
            message="✓ File restored and verified"
        )

        return data

    def download_file(self,
        metadata_path: str,
        output_path: str,
        strict: bool = False,
        public_key_path: str = None):

        self.log("=" * 60)
        self.log(f"DOWNLOADING from metadata: {metadata_path}")
        self.log("=" * 60)

        metadata = file_chunker.FileMetadata.load(metadata_path)

        data = self.download_bytes(
            metadata,
            strict=strict,
            public_key_path=public_key_path
        )

        with open(output_path, "wb") as f:
            f.write(data)

        self.log("=" * 60)
        self.log(f"DOWNLOAD COMPLETE: {output_path}")
        self.log("=" * 60)

        return True

    # ============================================================
    # OFFLINE METADATA VERIFICATION
    # ============================================================

    def verify_metadata_report(self,
        metadata: file_chunker.FileMetadata,
        public_key_path: str = None,
        strict: bool = False) -> dict:
        """
        Run the RFC-0005 Section 3.1 offline checks and return a structured
        report instead of raising, so a UI can render every check.

        `passed` is False when any check failed; in strict mode a warning
        also counts as a failure.
        """

        checks: List[Dict] = []

        def record(name: str, ok: bool, detail: str, fatal: bool = True):
            checks.append({
                "name": name,
                "ok": ok,
                "detail": detail,
                # Um aviso só derruba a verificação em modo estrito.
                "fatal": fatal
            })

        # 1. Assinatura RSA (RFC-0005 Seção 3.3, item 1)
        if not metadata.signature:
            record(
                "RSA signature",
                False,
                "Metadata is not signed (BSP v4 signature absent)",
                fatal=False
            )
        elif not public_key_path or not os.path.exists(public_key_path or ""):
            record(
                "RSA signature",
                False,
                "Metadata is signed but no public key is available to verify it",
                fatal=False
            )
        elif metadata.verify_signature(public_key_path):
            record("RSA signature", True, f"Valid (verified with {public_key_path})")
        else:
            record(
                "RSA signature",
                False,
                "Signature verification FAILED — metadata was modified after "
                "signing, or the wrong public key was used"
            )

        # 2. Campos obrigatórios (RFC-0005 Seção 3.3, item 3)
        required_fields = [
            "filename", "original_size", "file_hash",
            "chunk_count", "chunks", "protocol_version"
        ]
        missing = [
            f for f in required_fields
            if getattr(metadata, f, None) is None
        ]
        record(
            "Required fields",
            not missing,
            "All present" if not missing else f"Missing: {', '.join(missing)}"
        )

        # 3. Consistência da contagem de chunks (RFC-0005 Seção 3.3, item 4)
        consistent = metadata.chunk_count == len(metadata.chunks)
        record(
            "Chunk count",
            consistent,
            f"Consistent ({metadata.chunk_count} chunks)" if consistent
            else (
                f"Mismatch: declared {metadata.chunk_count}, "
                f"found {len(metadata.chunks)} chunk entries"
            )
        )

        # 4. Formato dos hashes (RFC-0005 Seção 3.3, item 5)
        valid_hex_chars = set("0123456789abcdef")
        bad_hashes = [
            c.chunk_index for c in metadata.chunks
            if not c.chunk_hash
            or len(c.chunk_hash) != 64
            or not all(ch in valid_hex_chars for ch in c.chunk_hash)
        ]
        record(
            "Chunk SHA-256 format",
            not bad_hashes,
            f"All {len(metadata.chunks)} entries carry a valid 64-char hash"
            if not bad_hashes
            else f"Invalid or missing hash at chunk(s): {_summarize(bad_hashes)}"
        )

        # 5. Estrutura de coordenadas (RFC-0005 Seção 3.3, item 6)
        required_coord_keys = {"hex", "wall", "shelf", "volume", "page"}
        incomplete = [
            c.chunk_index for c in metadata.chunks
            if c.babel_coords and (required_coord_keys - set(c.babel_coords.keys()))
        ]
        not_uploaded = [c.chunk_index for c in metadata.chunks if not c.babel_coords]

        if incomplete:
            record(
                "Coordinate structure",
                False,
                f"Incomplete coordinates at chunk(s): {_summarize(incomplete)}"
            )
        elif not_uploaded:
            record(
                "Coordinate structure",
                False,
                f"{len(not_uploaded)} chunk(s) have no coordinates — "
                f"this file cannot be restored",
                fatal=False
            )
        else:
            record("Coordinate structure", True, "All coordinates complete")

        # 6. Raiz Merkle (BSP v6 / RFC-0007). Recompõe a raiz a partir dos
        #    hashes armazenados e compara com a raiz registrada — offline.
        if not metadata.merkle_root:
            record(
                "Merkle root",
                False,
                "No Merkle root (metadata predates BSP v6)",
                fatal=False
            )
        elif metadata.verify_merkle_root():
            record(
                "Merkle root",
                True,
                f"Matches the chunk-hash tree (height {metadata.merkle_height})"
            )
        else:
            record(
                "Merkle root",
                False,
                "Recomputed root does not match — the chunk-hash list was altered"
            )

        passed = all(
            c["ok"] or (not c["fatal"] and not strict)
            for c in checks
        )

        return {
            "passed": passed,
            "strict": strict,
            "protocol_version": metadata.protocol_version,
            "signed": bool(metadata.signature),
            "merkle_root": metadata.merkle_root,
            "merkle_height": metadata.merkle_height,
            "checks": checks
        }

    def verify_metadata_only(self,
        metadata_path: str,
        public_key_path: str,
        strict: bool = False):

        metadata = file_chunker.FileMetadata.load(metadata_path)

        # 1. Verificar assinatura RSA (RFC-0005 Seção 3.3, item 1)
        self.log("Verifying metadata signature...")

        if not metadata.verify_signature(public_key_path):
            raise RuntimeError("Invalid metadata signature")

        self.log("✓ Signature valid")

        report = self.verify_metadata_report(
            metadata,
            public_key_path=public_key_path,
            strict=strict
        )

        for check in report["checks"]:
            if check["name"] == "RSA signature":
                continue

            if check["ok"]:
                self.log(f"✓ {check['name']}: {check['detail']}")
            elif strict or check["fatal"]:
                raise RuntimeError(f"{check['name']}: {check['detail']}")
            else:
                self.log(f"WARNING: {check['name']}: {check['detail']}", "WARNING")

        self.log("✓ Metadata structure verified")
        return True

    # ============================================================
    # PARTIAL VERIFICATION (BSP v6 / RFC-0007)
    # ============================================================

    def verify_chunk_report(self,
        metadata: file_chunker.FileMetadata,
        index: int,
        public_key_path: str = None) -> dict:
        """
        Prove that a single chunk is authentic against the Merkle root by
        retrieving ONLY that chunk from Babel and checking its inclusion
        proof — the headline BSP v6 benefit (no full download required).

        Returns a structured report. Network access is limited to the one
        chunk being verified.
        """

        if not metadata.merkle_root:
            raise RuntimeError(
                "Metadata has no Merkle root (pre-BSP v6); "
                "partial verification is unavailable"
            )

        if not 0 <= index < metadata.chunk_count:
            raise RuntimeError(
                f"Chunk index {index} out of range (0..{metadata.chunk_count - 1})"
            )

        # A assinatura protege a raiz; verifique-a quando houver chave.
        signature_checked = False
        if public_key_path and metadata.signature:
            if not metadata.verify_signature(public_key_path):
                raise RuntimeError("Invalid metadata signature")
            signature_checked = True

        chunk = metadata.chunks[index]
        coords = chunk.babel_coords
        if not coords:
            raise RuntimeError(f"Missing coordinates for chunk {index}")

        self.log(f"Retrieving only chunk {index} for partial verification...")
        self.emit(
            stage="verify-chunk",
            status="running",
            current_chunk=index + 1,
            total_chunks=metadata.chunk_count,
            message=f"Retrieving chunk {index} from Babel..."
        )

        encoded = babel.browse(
            coords["hex"], coords["wall"], coords["shelf"],
            coords["volume"], coords["page"]
        )
        if not encoded:
            raise RuntimeError(f"Failed to retrieve chunk {index} from Babel")

        data = binary_encoder.decode_babel_to_bytes(encoded)[:chunk.chunk_size]
        retrieved_hash = hashlib.sha256(data).hexdigest()

        # A folha é o hash do chunk RECUPERADO; se ele foi adulterado, a
        # prova (montada com os hashes irmãos da metadata) não reconstrói a raiz.
        proof = metadata.chunk_proof(index)
        proof_ok = merkle.verify_proof_hex(
            retrieved_hash, index, proof, metadata.merkle_root
        )
        hash_matches = retrieved_hash == chunk.chunk_hash

        authentic = proof_ok and hash_matches

        if authentic:
            self.log(f"✓ Chunk {index} is authentic (Merkle proof verified)")
        else:
            self.log(f"✗ Chunk {index} FAILED partial verification", "WARNING")

        return {
            "index": index,
            "authentic": authentic,
            "hash_matches": hash_matches,
            "proof_valid": proof_ok,
            "signature_checked": signature_checked,
            "expected_hash": chunk.chunk_hash,
            "retrieved_hash": retrieved_hash,
            "merkle_root": metadata.merkle_root,
            "proof_length": len(proof),
        }

    def verify_chunk_only(self,
        metadata_path: str,
        index: int,
        public_key_path: str = None,
        strict: bool = False) -> bool:

        metadata = file_chunker.FileMetadata.load(metadata_path)
        report = self.verify_chunk_report(
            metadata, index, public_key_path=public_key_path
        )

        self.log(
            f"Expected hash : {report['expected_hash']}"
        )
        self.log(
            f"Retrieved hash: {report['retrieved_hash']}"
        )
        self.log(
            f"Proof length  : {report['proof_length']} "
            f"(vs {metadata.chunk_count} chunks — no full download)"
        )

        if report["authentic"]:
            self.log(f"✓ Chunk {index} verified against Merkle root")
            return True

        raise RuntimeError(
            f"Chunk {index} SHA256 mismatch — partial verification failed"
        )

    # ============================================================
    # INFO
    # ============================================================

    def metadata_info(self, metadata: file_chunker.FileMetadata) -> dict:
        """Structured equivalent of the `info` command."""

        return {
            "filename": metadata.filename,
            "original_size": metadata.original_size,
            "file_hash": metadata.file_hash,
            "chunk_count": metadata.chunk_count,
            "protocol_version": metadata.protocol_version,
            "signed": bool(metadata.signature),
            "merkle_root": metadata.merkle_root,
            "merkle_height": metadata.merkle_height,
            "uploaded_chunks": sum(
                1 for c in metadata.chunks if c.babel_coords
            ),
            "chunks": [
                {
                    "index": c.chunk_index,
                    "size": c.chunk_size,
                    "hash": c.chunk_hash,
                    "coords": c.babel_coords or None
                }
                for c in metadata.chunks
            ]
        }

    def list_metadata(self, metadata_path: str):

        metadata = file_chunker.FileMetadata.load(metadata_path)
        info = self.metadata_info(metadata)

        print("\n" + "=" * 60)
        print("FILE INFORMATION")
        print("=" * 60)
        print(f"Filename: {info['filename']}")
        print(f"Size: {info['original_size']:,} bytes")
        print(f"File SHA256: {info['file_hash']}")
        print(f"Chunks: {info['chunk_count']}")
        print(f"Protocol: BSP {info['protocol_version']}")
        print(f"Signed: {'yes' if info['signed'] else 'no'}")
        if info["merkle_root"]:
            print(f"Merkle root: {info['merkle_root']} (height {info['merkle_height']})")
        else:
            print("Merkle root: none (pre-BSP v6)")

        print("\nCHUNKS")
        print("-" * 60)

        for chunk in info["chunks"]:
            coords = chunk["coords"]
            coord_str = (
                f"{coords['hex'][:8]}.../{coords['wall']}/"
                f"{coords['shelf']}/{coords['volume']}/{coords['page']}"
                if coords else "NOT UPLOADED"
            )

            print(
                f"[{chunk['index']:03d}] "
                f"{chunk['size']:6d} bytes | "
                f"{chunk['hash'][:12]}... | "
                f"{coord_str}"
            )

        print("=" * 60 + "\n")


# ============================================================
# ERROR HELPERS
# ============================================================

def _summarize(indexes: List[int], limit: int = 10) -> str:
    shown = ", ".join(str(i) for i in indexes[:limit])
    if len(indexes) > limit:
        shown += f" (+{len(indexes) - limit} more)"
    return shown


def _explain(error: Exception, chunk_index: int,
             max_retries: int = MAX_CHUNK_RETRIES) -> str:
    """Attach an actionable hint to a chunk transfer failure."""

    message = str(error)
    lowered = message.lower()

    hint = ""
    if "no coordinates" in lowered:
        hint = " | Hint: check that Babel is reachable and the chunk is not oversized."
    elif "timeout" in lowered or "timed out" in lowered:
        hint = " | Hint: the Babel server may be slow. Try again later."
    elif "rate limit" in lowered or "429" in lowered:
        hint = " | Hint: wait a few minutes before retrying."
    elif "connection" in lowered:
        hint = " | Hint: check your internet connection."

    return (
        f"chunk {chunk_index + 1} failed after {max_retries} attempts. "
        f"Last error: {message}{hint}"
    )


# ============================================================
# CLI
# ============================================================

def main():

    # Prefer real UTF-8 output where the stream supports switching.
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass

    parser = argparse.ArgumentParser(
        description="Babel File Storage - BSP v6 (SHA256 + Merkle)"
    )

    subparsers = parser.add_subparsers(dest="command")

    # Flags de política de retry, compartilhadas por upload e download.
    retry_parent = argparse.ArgumentParser(add_help=False)
    retry_parent.add_argument(
        "--max-retries", type=int, default=None, metavar="N",
        help=f"Attempts per chunk before giving up (default {MAX_CHUNK_RETRIES})"
    )
    retry_parent.add_argument(
        "--retry-delay", type=float, default=None, metavar="SECONDS",
        help=f"Initial backoff between retries, doubled each attempt "
             f"(default {INITIAL_RETRY_DELAY}s)"
    )

    # Upload
    upload_parser = subparsers.add_parser("upload", parents=[retry_parent])
    upload_parser.add_argument("file")
    upload_parser.add_argument("--metadata", required=True)
    upload_parser.add_argument("--privkey")
    upload_parser.add_argument(
        "--rate-limit", type=float, default=None, metavar="SECONDS",
        help=f"Delay between chunks (default {RATE_LIMIT_DELAY}s; 0 disables). "
             f"Be kind to libraryofbabel.info."
    )
    upload_parser.add_argument(
        "--no-resume", action="store_true",
        help="Ignore any existing partial metadata and upload every chunk again"
    )
    upload_parser.add_argument("--quiet", action="store_true")

    # Download
    download_parser = subparsers.add_parser("download", parents=[retry_parent])
    download_parser.add_argument("metadata")
    download_parser.add_argument("--output", required=True)
    download_parser.add_argument("--pubkey")
    download_parser.add_argument("--strict", action="store_true")
    download_parser.add_argument("--quiet", action="store_true")

    # Verificação ofline da metadata
    verify_parser = subparsers.add_parser("verify-metadata")
    verify_parser.add_argument("metadata")
    verify_parser.add_argument("--pubkey", required=True)
    verify_parser.add_argument("--strict", action="store_true")
    verify_parser.add_argument("--quiet", action="store_true")

    # Verificação parcial de um único chunk (BSP v6)
    verify_chunk_parser = subparsers.add_parser(
        "verify-chunk",
        help="Prove one chunk is authentic against the Merkle root "
             "(downloads only that chunk)"
    )
    verify_chunk_parser.add_argument("metadata")
    verify_chunk_parser.add_argument("--index", type=int, required=True)
    verify_chunk_parser.add_argument("--pubkey")
    verify_chunk_parser.add_argument("--strict", action="store_true")
    verify_chunk_parser.add_argument("--quiet", action="store_true")

    # Info
    info_parser = subparsers.add_parser("info")
    info_parser.add_argument("metadata")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    storage = BabelStorage(
        verbose=not getattr(args, "quiet", False),
        rate_limit_delay=getattr(args, "rate_limit", None),
        max_retries=getattr(args, "max_retries", None),
        retry_delay=getattr(args, "retry_delay", None)
    )

    try:

        if args.command == "upload":
            storage.upload_file(
                args.file,
                args.metadata,
                private_key_path=args.privkey,
                resume=not args.no_resume
            )

        elif args.command == "download":
            storage.download_file(
                args.metadata,
                args.output,
                strict=args.strict,
                public_key_path=args.pubkey
            )
            sys.exit(0)

        elif args.command == "verify-metadata":
            storage.verify_metadata_only(
                args.metadata,
                args.pubkey,
                strict=args.strict
            )

        elif args.command == "verify-chunk":
            storage.verify_chunk_only(
                args.metadata,
                args.index,
                public_key_path=args.pubkey,
                strict=args.strict
            )

        elif args.command == "info":
            storage.list_metadata(args.metadata)

    except RuntimeError as e:
        msg = str(e)
        print(f"[ERROR] {msg}", file=sys.stderr)
        sys.exit(exit_code_for(msg))

    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)


def exit_code_for(message: str) -> int:
    """Map a failure to the RFC-0005 Section 2.4 exit codes."""

    msg = message.lower()

    if "chunk" in msg and ("mismatch" in msg or "sha256" in msg):
        return 1  # chunk hash mismatch em modo estrito
    elif "final file sha256" in msg or "final file" in msg:
        return 2  # hash final do arquivo incorreto
    elif "signature" in msg:
        return 3  # assinatura inválida
    elif "missing" in msg or "coordinates" in msg or "not found" in msg:
        return 4  # dados ausentes
    return 1


if __name__ == "__main__":
    main()
