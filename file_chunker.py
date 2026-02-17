#!/usr/bin/env python3
"""
File Chunking System for BabelStorage

Aligned with:
BSP (BabelStorage Protocol) v5

Features:
- Zstandard compression
- Chunk splitting
- SHA256 per-chunk integrity (BSP v2)
- SHA256 full file integrity (BSP v1)
- RSA digital signature (metadata) (BSP v4)
- Strict verification compatible (BSP v5)
"""

import os
import hashlib
import json
import gzip
import zstandard as zstd
import crypto_utils

from typing import List, Dict, Tuple
from dataclasses import dataclass
from binary_encoder import calculate_overhead

# ============================================================
# GLOBAL CONFIGURATION
# ============================================================

MAX_BABEL_PAGE_SIZE = 3000
ENCODING_OVERHEAD = calculate_overhead()
MAX_CHUNK_BYTES = int(MAX_BABEL_PAGE_SIZE / ENCODING_OVERHEAD) - 8

# ============================================================
# CHUNK METADATA
# ============================================================

@dataclass
class ChunkMetadata:
    chunk_index: int
    chunk_size: int
    chunk_hash: str  # SHA256 hex
    babel_coords: Dict[str, str]

    def to_compact_list(self) -> List:
        """
        Compact format:
        [size, sha256, hex, wall, shelf, volume, page]
        """
        if not self.babel_coords:
            return [self.chunk_size, self.chunk_hash]

        return [
            self.chunk_size,
            self.chunk_hash,
            self.babel_coords.get("hex", ""),
            int(self.babel_coords.get("wall", 0)),
            int(self.babel_coords.get("shelf", 0)),
            int(self.babel_coords.get("volume", 0)),
            int(self.babel_coords.get("page", 0)),
        ]

    @classmethod
    def from_compact_list(cls, index: int, data: List):
        coords = {}

        if len(data) >= 7:
            coords = {
                "hex": data[2],
                "wall": str(data[3]),
                "shelf": str(data[4]),
                "volume": str(data[5]),
                "page": str(data[6]),
            }

        return cls(
            chunk_index=index,
            chunk_size=data[0],
            chunk_hash=data[1],
            babel_coords=coords,
        )

# ============================================================
# FILE METADATA
# ============================================================

@dataclass
class FileMetadata:
    filename: str
    original_size: int
    file_hash: str  # SHA256 full file
    chunk_count: int
    chunks: List[ChunkMetadata]
    protocol_version: str = "v5"
    signature: str = ""

    # -----------------------------
    # SERIALIZATION
    # -----------------------------

    def to_dict(self) -> dict:
        return {
            "f": self.filename,
            "s": self.original_size,
            "h": self.file_hash,
            "c": self.chunk_count,
            "v": self.protocol_version,
            "chk": [c.to_compact_list() for c in self.chunks],
        }

    def to_signed_dict(self) -> dict:
        base = self.to_dict()
        if self.signature:
            base["sig"] = self.signature
        return base

    @classmethod
    def from_dict(cls, data: dict):
        chunks_raw = data.get("chk", [])
        chunks = [
            ChunkMetadata.from_compact_list(i, chunk)
            for i, chunk in enumerate(chunks_raw)
        ]

        return cls(
            filename=data["f"],
            original_size=data["s"],
            file_hash=data["h"],
            chunk_count=data["c"],
            chunks=chunks,
            protocol_version=data.get("v", "legacy"),
            signature=data.get("sig", ""),
        )

    # -----------------------------
    # SIGNATURE (RSA)
    # -----------------------------

    def sign(self, private_key_path: str):
        self.signature = crypto_utils.sign_metadata(
            self.to_dict(),
            private_key_path
        )

    def verify_signature(self, public_key_path: str) -> bool:
        if not self.signature:
            return False

        return crypto_utils.verify_metadata_signature(
            self.to_dict(),
            public_key_path,
            self.signature
        )

    # -----------------------------
    # SAVE / LOAD
    # -----------------------------

    def save(self, filepath: str):
        if not filepath.endswith(".gz"):
            filepath += ".gz"

        with gzip.open(filepath, "wt", encoding="utf-8") as f:
            json.dump(self.to_signed_dict(), f, separators=(",", ":"))

    @classmethod
    def load(cls, filepath: str):
        if not filepath.endswith(".gz") and os.path.exists(filepath + ".gz"):
            filepath += ".gz"

        with gzip.open(filepath, "rt", encoding="utf-8") as f:
            data = json.load(f)

        return cls.from_dict(data)

# ============================================================
# UTILITIES
# ============================================================

def calculate_file_hash(filepath: str) -> str:
    sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256.update(chunk)
    return sha256.hexdigest()

# ============================================================
# SPLIT + COMPRESS
# ============================================================

def split_file_into_chunks(filepath: str,
                           chunk_size: int = MAX_CHUNK_BYTES):

    with open(filepath, "rb") as f:
        data = f.read()

    cctx = zstd.ZstdCompressor(level=19)
    compressed_data = cctx.compress(data)

    for chunk_index, offset in enumerate(
        range(0, len(compressed_data), chunk_size)
    ):
        yield chunk_index, compressed_data[offset:offset + chunk_size]

# ============================================================
# RECONSTRUCTION
# ============================================================

def reconstruct_file_from_chunks(
    chunks_data: List[Tuple[int, bytes]],
    metadata: FileMetadata,
    output_filepath: str,
    strict: bool = False,
):
    """
    Reconstruct original file from compressed chunks.
    """

    if len(chunks_data) != metadata.chunk_count:
        raise RuntimeError("Chunk count mismatch.")

    sorted_chunks = sorted(chunks_data, key=lambda x: x[0])
    compressed_data = bytearray()

    for index, chunk_data in sorted_chunks:

        if index >= metadata.chunk_count:
            raise RuntimeError(f"Invalid chunk index {index}")

        expected_hash = metadata.chunks[index].chunk_hash
        actual_hash = hashlib.sha256(chunk_data).hexdigest()

        if actual_hash != expected_hash:
            if strict:
                raise RuntimeError(
                    f"SHA256 mismatch at chunk {index}"
                )
            else:
                print(f"Warning: SHA256 mismatch at chunk {index}")

        compressed_data.extend(chunk_data)

    dctx = zstd.ZstdDecompressor()

    try:
        decompressed_data = dctx.decompress(bytes(compressed_data))
    except Exception as e:
        raise RuntimeError(f"Decompression failed: {e}")

    final_hash = hashlib.sha256(decompressed_data).hexdigest()

    if final_hash != metadata.file_hash:
        raise RuntimeError("Final file SHA256 mismatch.")

    with open(output_filepath, "wb") as f:
        f.write(decompressed_data)

# ============================================================
# METADATA CREATION
# ============================================================

def create_file_metadata(filepath: str) -> FileMetadata:

    filename = os.path.basename(filepath)
    file_size = os.path.getsize(filepath)
    file_hash = calculate_file_hash(filepath)

    chunks = []

    for chunk_index, chunk_data in split_file_into_chunks(filepath):

        chunk_hash = hashlib.sha256(chunk_data).hexdigest()

        chunks.append(
            ChunkMetadata(
                chunk_index=chunk_index,
                chunk_size=len(chunk_data),
                chunk_hash=chunk_hash,
                babel_coords={},
            )
        )

    return FileMetadata(
        filename=filename,
        original_size=file_size,
        file_hash=file_hash,
        chunk_count=len(chunks),
        chunks=chunks,
        protocol_version="v5",
    )

# ============================================================
# INTEGRITY CHECK
# ============================================================

def verify_file_integrity(filepath: str,
                          metadata: FileMetadata) -> bool:

    return (
        os.path.getsize(filepath) == metadata.original_size
        and calculate_file_hash(filepath) == metadata.file_hash
    )

# ============================================================
# STORAGE ESTIMATION
# ============================================================

def estimate_storage_requirements(filepath: str) -> dict:

    file_size = os.path.getsize(filepath)

    # Estimar tamanho comprimido para calcular número real de chunks.
    # Chunks são fatias de dados Zstd comprimidos, não do arquivo original.
    with open(filepath, "rb") as f:
        raw_data = f.read()

    import zstandard as zstd
    cctx = zstd.ZstdCompressor(level=19)
    compressed_size = len(cctx.compress(raw_data))

    chunk_count = (compressed_size + MAX_CHUNK_BYTES - 1) // MAX_CHUNK_BYTES
    estimated_encoded_size = int(compressed_size * ENCODING_OVERHEAD)

    return {
        "original_size_bytes": file_size,
        "original_size_mb": file_size / 1024 / 1024,
        "compressed_size_bytes": compressed_size,
        "compressed_size_mb": compressed_size / 1024 / 1024,
        "encoding_overhead": ENCODING_OVERHEAD,
        "max_chunk_bytes": MAX_CHUNK_BYTES,
        "chunk_count": chunk_count,
        "estimated_encoded_size_bytes": estimated_encoded_size,
        "estimated_encoded_size_mb": estimated_encoded_size / 1024 / 1024,
        "estimated_upload_time_seconds": chunk_count * 2,
        "estimated_download_time_seconds": chunk_count * 1,
        "coordinates_storage_bytes_estimate": chunk_count * 50,
    }