#!/usr/bin/env python3
"""
Merkle tree utilities for BabelStorage (BSP v6).

BSP v6 records the root of a binary SHA-256 Merkle tree built over the
per-chunk hashes. That single 32-byte root — covered by the RSA signature
when the metadata is signed (BSP v4) — lets a verifier prove that one
specific chunk is authentic by retrieving only that chunk plus a short
inclusion proof, instead of downloading the whole file (RFC 0007).

Construction (matches RFC 0006 Section 1.1):
- Leaves are the raw SHA-256 digests of each chunk, in chunk order.
- At every level, adjacent nodes are hashed as sha256(left + right).
- When a level has an odd number of nodes, the last node is paired with
  itself (duplicated).

The leaves are exactly the per-chunk hashes already stored in the
metadata, so the whole tree — and any inclusion proof — can be rebuilt
offline from the metadata alone, with no access to the Library of Babel.
"""

import hashlib
from typing import List, Tuple

# An inclusion proof is an ordered list of (sibling_digest, sibling_is_left)
# pairs, one per level from the leaf up to (but not including) the root.
Proof = List[Tuple[bytes, bool]]


def _hash_pair(left: bytes, right: bytes) -> bytes:
    return hashlib.sha256(left + right).digest()


def build_levels(leaves: List[bytes]) -> List[List[bytes]]:
    """
    Build every level of the tree, bottom-up.

    Returns a list whose first entry is the leaf level and whose last
    entry is the single-node root level. Raises ValueError on empty input
    (a file always has at least one chunk).
    """
    if not leaves:
        raise ValueError("Cannot build a Merkle tree with no leaves")

    levels = [list(leaves)]

    while len(levels[-1]) > 1:
        current = levels[-1]
        parents = []
        for i in range(0, len(current), 2):
            left = current[i]
            right = current[i + 1] if i + 1 < len(current) else left
            parents.append(_hash_pair(left, right))
        levels.append(parents)

    return levels


def compute_root(leaves: List[bytes]) -> bytes:
    """Return the raw 32-byte Merkle root for these leaves."""
    return build_levels(leaves)[-1][0]


def tree_height(leaf_count: int) -> int:
    """
    Number of levels above the leaves (0 for a single-leaf tree).

    Equivalent to ceil(log2(leaf_count)).
    """
    if leaf_count <= 1:
        return 0

    height = 0
    nodes = leaf_count
    while nodes > 1:
        nodes = (nodes + 1) // 2
        height += 1
    return height


def build_proof(leaves: List[bytes], index: int) -> Proof:
    """
    Build the inclusion proof for the leaf at `index`.

    The proof is the list of sibling digests needed to recompute the root,
    together with each sibling's side, from the leaf upward.
    """
    if not 0 <= index < len(leaves):
        raise IndexError(f"Leaf index {index} out of range (0..{len(leaves) - 1})")

    proof: Proof = []
    levels = build_levels(leaves)

    for level in levels[:-1]:  # every level except the root
        if index % 2 == 0:
            # sibling is to the right; duplicate self when it is missing
            sibling_index = index + 1
            sibling = level[sibling_index] if sibling_index < len(level) else level[index]
            proof.append((sibling, False))
        else:
            # sibling is to the left
            proof.append((level[index - 1], True))
        index //= 2

    return proof


def verify_proof(leaf: bytes, index: int, proof: Proof, root: bytes) -> bool:
    """
    Recompute the root from a leaf and its inclusion proof.

    `index` is only used as a sanity check against the sides recorded in
    the proof; the folding itself is driven by the proof.
    """
    computed = leaf

    for sibling, sibling_is_left in proof:
        if sibling_is_left:
            computed = _hash_pair(sibling, computed)
        else:
            computed = _hash_pair(computed, sibling)

    return computed == root


# =============================================================
# HEX CONVENIENCE WRAPPERS
#
# Metadata stores hashes as 64-char hex strings, so these operate on
# hex throughout and keep the raw-bytes core above easy to test.
# =============================================================

def _leaves_from_hex(hash_hexes: List[str]) -> List[bytes]:
    return [bytes.fromhex(h) for h in hash_hexes]


def compute_root_hex(chunk_hashes: List[str]) -> str:
    """Merkle root (hex) over a list of hex chunk hashes."""
    return compute_root(_leaves_from_hex(chunk_hashes)).hex()


def build_proof_hex(chunk_hashes: List[str], index: int) -> List[dict]:
    """
    Inclusion proof for one chunk, as JSON-friendly dicts:
        [{"hash": "<hex>", "left": bool}, ...]
    """
    proof = build_proof(_leaves_from_hex(chunk_hashes), index)
    return [{"hash": sib.hex(), "left": is_left} for sib, is_left in proof]


def verify_proof_hex(leaf_hash_hex: str, index: int,
                     proof_hex: List[dict], root_hex: str) -> bool:
    """Verify a hex inclusion proof produced by build_proof_hex."""
    proof: Proof = [
        (bytes.fromhex(step["hash"]), bool(step["left"]))
        for step in proof_hex
    ]
    return verify_proof(
        bytes.fromhex(leaf_hash_hex),
        index,
        proof,
        bytes.fromhex(root_hex)
    )
