#!/usr/bin/env python3
"""
Binary Encoder for BabelStorage (BSP Compatible)

Features:
- Base29 encoding using Babel alphabet
- Versioned prefix (v1–v4 compatible structure)
- Legacy support (no prefix)
- Deterministic reversible encoding
- Structural validation
"""

import math

# =============================
# CONSTANTS
# =============================

BABEL_ALPHABET = "abcdefghijklmnopqrstuvwxyz .,"
BASE = len(BABEL_ALPHABET)

_CHAR_TO_IDX = {c: i for i, c in enumerate(BABEL_ALPHABET)}
_IDX_TO_CHAR = {i: c for i, c in enumerate(BABEL_ALPHABET)}

SUPPORTED_VERSIONS = ('a', 'b', 'c', 'd')  # v1..v5


# =============================
# BASE29 INTEGER ENCODING
# =============================

def _encode_base29_int(value: int) -> str:
    if value < 0:
        raise ValueError("Negative integers not supported.")

    if value == 0:
        return _IDX_TO_CHAR[0]

    digits = []
    while value > 0:
        digits.append(_IDX_TO_CHAR[value % BASE])
        value //= BASE

    return "".join(reversed(digits))


def _decode_base29_int(text: str) -> int:
    if not text:
        raise ValueError("Empty base29 string.")

    value = 0
    for ch in text:
        if ch not in _CHAR_TO_IDX:
            raise ValueError(f"Invalid character in base29 string: {ch}")
        value = value * BASE + _CHAR_TO_IDX[ch]

    return value


# =============================
# MAIN ENCODER
# =============================

def encode_bytes_to_babel(data: bytes) -> str:
    """
    Encodes arbitrary bytes into base29 text with structural prefix.
    Compatible with BSP v1–v4 decoding.
    """

    if not data:
        return ""

    byte_len = len(data)
    value = int.from_bytes(data, 'big')

    # Encode body
    body = _encode_base29_int(value)
    body_len = len(body)

    # Encode byte length
    byte_len_encoded = _encode_base29_int(byte_len)
    byte_len_size = len(byte_len_encoded)

    # Encode body length
    body_len_encoded = _encode_base29_int(body_len)
    body_len_size = len(body_len_encoded)

    # Safety validation (prevent overflow)
    if byte_len_size >= BASE or body_len_size >= BASE:
        raise ValueError("Prefix length overflow — input too large.")

    prefix = (
        _IDX_TO_CHAR[byte_len_size] +
        byte_len_encoded +
        _IDX_TO_CHAR[body_len_size] +
        body_len_encoded
    )

    # Version marker: 'd' for v1–v5 compatible structured encoding
    return 'd' + prefix + body


# =============================
# DECODER
# =============================

def decode_babel_to_bytes(encoded_text: str) -> bytes:
    """
    Decoder compatible with:
    - BSP versioned encoding (v1–v5)
    - Legacy encoding (no prefix)
    """

    if not encoded_text:
        return b""

    # Remove linhas e carriage returns (sanitization)
    encoded_text = encoded_text.replace('\n', '').replace('\r', '')

    # Sanitização: Verifica caracteres válidos e texto não vazio
    allowed = set(BABEL_ALPHABET)
    for c in encoded_text:
        if c not in allowed:
            raise ValueError(f"Invalid character: {c}")

    if not encoded_text:
        raise ValueError("Empty text after sanitization.")

    version = encoded_text[0]

    # Versioned decoding
    if version in SUPPORTED_VERSIONS:
        return _decode_v1(encoded_text[1:])

    # Legacy decoding (no prefix)
    return _decode_legacy(encoded_text)


# =============================
# STRUCTURED DECODER (v1–v5)
# =============================

def _decode_v1(text: str) -> bytes:
    """
    Structured decoding with prefix:
    [byte_len_size][byte_len_encoded]
    [body_len_size][body_len_encoded]
    [body]
    """

    if not text:
        raise ValueError("Truncated encoded text.")

    pos = 0

    # byte_len_size
    byte_len_size = _CHAR_TO_IDX[text[pos]]
    pos += 1

    if pos + byte_len_size > len(text):
        raise ValueError("Truncated byte length field.")

    byte_len_encoded = text[pos:pos + byte_len_size]
    pos += byte_len_size
    byte_len = _decode_base29_int(byte_len_encoded)

    # body_len_size
    if pos >= len(text):
        raise ValueError("Missing body length size field.")

    body_len_size = _CHAR_TO_IDX[text[pos]]
    pos += 1

    if pos + body_len_size > len(text):
        raise ValueError("Truncated body length field.")

    body_len_encoded = text[pos:pos + body_len_size]
    pos += body_len_size
    body_len = _decode_base29_int(body_len_encoded)

    # body
    if pos + body_len > len(text):
        raise ValueError("Truncated body.")

    body = text[pos:pos + body_len]

    value = _decode_base29_int(body)

    try:
        return value.to_bytes(byte_len, 'big')
    except OverflowError:
        raise ValueError("Decoded integer does not match declared byte length.")


# =============================
# LEGACY DECODER
# =============================

def _decode_legacy(text: str) -> bytes:
    """
    Legacy decoding without prefix.
    """

    value = _decode_base29_int(text)

    if value == 0:
        return b"\x00"

    byte_len = (value.bit_length() + 7) // 8
    return value.to_bytes(byte_len, 'big')


# =============================
# SIZE ESTIMATION
# =============================

def calculate_overhead() -> float:
    """
    Theoretical base29 encoding overhead:
    bits per char = log2(BASE)
    bytes per char = 8 / log2(BASE)
    """
    return 8.0 / math.log2(BASE)


def estimate_encoded_size(file_size_bytes: int) -> int:
    """
    Rough estimation of encoded size (not including prefix overhead).
    """
    return int(math.ceil(file_size_bytes * calculate_overhead()))
