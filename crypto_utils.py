#!/usr/bin/env python3

"""
Crypto Utilities for Babel Storage

Functions for generating RSA keys, signing metadata, and verifying signatures.
"""


import json
import base64
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend


def generate_keys(private_path="private.pem", public_path="public.pem"):
    from cryptography.hazmat.primitives.asymmetric import rsa

    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=4096,
        backend=default_backend()
    )

    with open(private_path, "wb") as f:
        f.write(
            private_key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption()
            )
        )

    public_key = private_key.public_key()

    with open(public_path, "wb") as f:
        f.write(
            public_key.public_bytes(
                serialization.Encoding.PEM,
                serialization.PublicFormat.SubjectPublicKeyInfo
            )
        )


# =============================
# SIGN
# =============================

def sign_metadata(metadata_dict: dict, private_key_path: str) -> str:

    with open(private_key_path, "rb") as f:
        private_key = serialization.load_pem_private_key(
            f.read(), password=None, backend=default_backend()
        )

    serialized = json.dumps(
        metadata_dict,
        sort_keys=True,
        separators=(",", ":")
    ).encode()

    signature = private_key.sign(
        serialized,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH
        ),
        hashes.SHA256()
    )

    return base64.b64encode(signature).decode()


# =============================
# VERIFY
# =============================

def verify_metadata_signature(
    metadata_dict: dict,
    public_key_path: str,
    signature_b64: str
) -> bool:

    with open(public_key_path, "rb") as f:
        public_key = serialization.load_pem_public_key(
            f.read(), backend=default_backend()
        )

    serialized = json.dumps(
        metadata_dict,
        sort_keys=True,
        separators=(",", ":")
    ).encode()

    signature = base64.b64decode(signature_b64)

    try:
        public_key.verify(
            signature,
            serialized,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        return True
    except Exception:
        return False
