#!/usr/bin/env python3
"""
Babel File Storage System (BSP v5)

Implements:
- BSP v1: Full file SHA256 integrity
- BSP v2: Per-chunk SHA256 integrity
- BSP v4: RSA metadata signature
- BSP v5: Strict mode + offline verification
"""

import os
import sys
import time
import argparse
import hashlib
from typing import List, Tuple

import binary_encoder
import file_chunker
import babel


class BabelStorage:
    """Main class for file storage operations."""

    def __init__(self, verbose: bool = True):
        self.verbose = verbose

    def log(self, message: str, level: str = "INFO"):
        if self.verbose:
            print(f"[{level}] {message}")

    # ============================================================
    # UPLOAD
    # ============================================================

    def upload_file(self, filepath: str,
                    metadata_output: str = None,
                    private_key_path: str = None):

        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File not found: {filepath}")

        self.log("=" * 60)
        self.log(f"UPLOADING: {filepath}")
        self.log("=" * 60)

        metadata = file_chunker.create_file_metadata(filepath)

        self.log(f"File: {metadata.filename}")
        self.log(f"Size: {metadata.original_size:,} bytes")
        self.log(f"SHA256: {metadata.file_hash}")
        self.log(f"Chunks: {metadata.chunk_count}")

        start_time = time.time()

        for chunk_index, chunk_data in file_chunker.split_file_into_chunks(filepath):

            self.log(f"\nChunk {chunk_index + 1}/{metadata.chunk_count}")

            encoded = binary_encoder.encode_bytes_to_babel(chunk_data)

            max_retries = 4
            retry_delay = 2
            success = False

            for attempt in range(max_retries):
                try:
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
                    retrieved = babel.browse(hex_id, wall, shelf, volume, page)

                    if not retrieved:
                        raise RuntimeError("Verification browse failed")

                    retrieved_clean = retrieved.replace("\n", "").replace("\r", "")
                    if retrieved_clean[:len(encoded)] != encoded:
                        raise RuntimeError("Encoded data mismatch after upload")

                    self.log("✓ Verified successfully")
                    success = True
                    time.sleep(1.5)
                    break

                except Exception as e:
                    if attempt < max_retries - 1:
                        self.log(f"Attempt {attempt+1} failed: {e}", "WARNING")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                    else:
                        raise RuntimeError(f"Upload failed: {e}")

            if not success:
                raise RuntimeError("Upload aborted due to repeated failures.")

        total_time = time.time() - start_time
        self.log(f"\nUpload completed in {total_time:.2f}s")

        # Assina a metadata (BSP v4)
        if private_key_path:
            self.log("Signing metadata (RSA)...")
            metadata.sign(private_key_path)

        if metadata_output:
            metadata.save(metadata_output)
            self.log(f"Metadata saved to {metadata_output}")

        return metadata

    # ============================================================
    # DOWNLOAD
    # ============================================================

    def download_file(self,
        metadata_path: str,
        output_path: str,
        strict: bool = False,
        public_key_path: str = None):

        self.log("=" * 60)
        self.log(f"DOWNLOADING from metadata: {metadata_path}")
        self.log("=" * 60)

        metadata = file_chunker.FileMetadata.load(metadata_path)

        # Verificação de assinatura (BSP v4)
        if public_key_path:
            self.log("Verifying metadata signature...")
            if not metadata.verify_signature(public_key_path):
                raise RuntimeError("Invalid metadata digital signature.")
            self.log("✓ Signature verified")

        self.log(f"File: {metadata.filename}")
        self.log(f"Expected SHA256: {metadata.file_hash}")
        self.log(f"Chunks: {metadata.chunk_count}")

        chunks_data: List[Tuple[int, bytes]] = []

        for chunk in metadata.chunks:

            coords = chunk.babel_coords
            if not coords:
                raise RuntimeError(f"Missing coordinates for chunk {chunk.chunk_index}")

            self.log(f"\nRetrieving chunk {chunk.chunk_index + 1}")

            encoded = babel.browse(
                coords["hex"],
                coords["wall"],
                coords["shelf"],
                coords["volume"],
                coords["page"]
            )

            if not encoded:
                raise RuntimeError("Failed to retrieve chunk from Babel")

            chunk_data = binary_encoder.decode_babel_to_bytes(encoded)
            chunk_data = chunk_data[:chunk.chunk_size]

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
            else:
                self.log("✓ Chunk SHA256 verified")

            chunks_data.append((chunk.chunk_index, chunk_data))

        if len(chunks_data) != metadata.chunk_count:
            raise RuntimeError("Chunk count mismatch during download")

        # Reconstrução manual para BytesIO (BSP v5)
        self.log("\nReconstructing file...")
        file_chunker.reconstruct_file_from_chunks(
            chunks_data,
            metadata,
            output_path,
            strict=strict
        )

        # Verificação final do arquivo completo (BSP v1)
        self.log("Verifying final file SHA256...")

        with open(output_path, "rb") as f:
            final_hash = hashlib.sha256(f.read()).hexdigest()

        if final_hash != metadata.file_hash:
            raise RuntimeError("Final file SHA256 mismatch.")
        else:
            self.log("✓ Final file integrity verified")

        self.log("=" * 60)
        self.log(f"DOWNLOAD COMPLETE: {output_path}")
        self.log("=" * 60)

        return True

    # ============================================================
    # OFFLINE METADATA VERIFICATION
    # ============================================================

    def verify_metadata_only(self,
        metadata_path: str,
        public_key_path: str,
        strict: bool = False):

        metadata = file_chunker.FileMetadata.load(metadata_path)

        self.log("Verifying metadata signature...")

        if not metadata.verify_signature(public_key_path):
            raise RuntimeError("Invalid metadata signature")

        self.log("✓ Signature valid")

        if metadata.chunk_count != len(metadata.chunks):
            raise RuntimeError("Chunk count mismatch in metadata")

        for chunk in metadata.chunks:
            if not chunk.chunk_hash:
                if strict:
                    raise RuntimeError(
                        f"Missing SHA256 for chunk {chunk.chunk_index}"
                    )
                self.log("WARNING: Missing chunk SHA256", "WARNING")

        self.log("✓ Metadata structure verified")
        return True

    # ============================================================
    # INFO
    # ============================================================

    def list_metadata(self, metadata_path: str):

        metadata = file_chunker.FileMetadata.load(metadata_path)

        print("\n" + "=" * 60)
        print("FILE INFORMATION")
        print("=" * 60)
        print(f"Filename: {metadata.filename}")
        print(f"Size: {metadata.original_size:,} bytes")
        print(f"File SHA256: {metadata.file_hash}")
        print(f"Chunks: {metadata.chunk_count}")

        print("\nCHUNKS")
        print("-" * 60)

        for chunk in metadata.chunks:
            coords = chunk.babel_coords
            coord_str = (
                f"{coords['hex'][:8]}.../{coords['wall']}/"
                f"{coords['shelf']}/{coords['volume']}/{coords['page']}"
                if coords else "NOT UPLOADED"
            )

            print(
                f"[{chunk.chunk_index:03d}] "
                f"{chunk.chunk_size:6d} bytes | "
                f"{chunk.chunk_hash[:12]}... | "
                f"{coord_str}"
            )

        print("=" * 60 + "\n")


# ============================================================
# CLI
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description="Babel File Storage - BSP v5 (SHA256)"
    )

    subparsers = parser.add_subparsers(dest="command")

    # Upload
    upload_parser = subparsers.add_parser("upload")
    upload_parser.add_argument("file")
    upload_parser.add_argument("--metadata", required=True)
    upload_parser.add_argument("--privkey")
    upload_parser.add_argument("--quiet", action="store_true")

    # Download
    download_parser = subparsers.add_parser("download")
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

    # Info
    info_parser = subparsers.add_parser("info")
    info_parser.add_argument("metadata")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    storage = BabelStorage(verbose=not getattr(args, "quiet", False))

    try:

        if args.command == "upload":
            storage.upload_file(
                args.file,
                args.metadata,
                private_key_path=args.privkey
            )

        elif args.command == "download":
            success = storage.download_file(
                args.metadata,
                args.output,
                strict=args.strict,
                public_key_path=args.pubkey
            )
            sys.exit(0 if success else 1)

        elif args.command == "verify-metadata":
            storage.verify_metadata_only(
                args.metadata,
                args.pubkey,
                strict=args.strict
            )

        elif args.command == "info":
            storage.list_metadata(args.metadata)

    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
