#!/usr/bin/env python3
"""
Library of Babel API wrapper.

Provides programmatic access to:
https://libraryofbabel.info

Refactored for robustness, validation safety, and production use.
"""

__author__ = "Victor Barros"
__maintainer__ = "Sonael Neto"
__version__ = "2.0"

import re
import random
import time
from typing import Optional, Tuple
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup


# ==============================
# CONFIGURATION
# ==============================

BASE_URL = "https://libraryofbabel.info"
SEARCH_URL = f"{BASE_URL}/search.cgi"
BROWSE_URL = f"{BASE_URL}/book.cgi"

REQUEST_TIMEOUT = 60
HEXAGON_LENGTH = 3200

VALID_ALPHABET = "abcdefghijklmnopqrstuvwxyz"
VALID_NUMBERS = "0123456789"

USER_AGENT = "LibraryOfBabelPythonWrapper/2.1"

# IMPORTANTE: Limite de caracteres do Babel
MAX_SEARCH_LENGTH = 3200  # Limite da página do Babel


# ==============================
# EXCEPTIONS
# ==============================

class BabelError(Exception):
    """Base exception for Babel-related errors."""
    pass


class ValidationError(BabelError):
    """Raised when input validation fails."""
    pass


class SearchError(BabelError):
    """Raised when search operation fails."""
    pass


class BrowseError(BabelError):
    """Raised when browse operation fails."""
    pass


class TextTooLongError(BabelError):
    """Raised when text exceeds Babel's limit."""
    pass


# ==============================
# SESSION WITH RETRY
# ==============================

def _create_session() -> requests.Session:
    session = requests.Session()

    retry_strategy = Retry(
        total=5,  # Aumentado de 3 para 5
        backoff_factor=2,  # Aumentado de 1 para 2
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST"]
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": USER_AGENT})

    return session


_session = _create_session()


# ==============================
# VALIDATION
# ==============================

def _validate_non_empty(value: str, field_name: str) -> None:
    if not value:
        raise ValidationError(f"{field_name} cannot be empty")


def _validate_numeric_range(value: str, min_val: int, max_val: int, field_name: str) -> int:
    if not value.isdigit():
        raise ValidationError(f"{field_name} must be numeric")

    int_value = int(value)

    if not (min_val <= int_value <= max_val):
        raise ValidationError(f"{field_name} must be between {min_val} and {max_val}")

    return int_value


def _validate_hexagon(hexagon: str) -> None:
    _validate_non_empty(hexagon, "Hexagon")

    if not all(c in VALID_ALPHABET + VALID_NUMBERS for c in hexagon):
        raise ValidationError("Hexagon must contain only lowercase alphanumeric characters")


def _validate_search_text(text: str, verbose: bool = False) -> None:
    """Valida o texto de busca."""
    
    if not text:
        raise ValidationError("Search text cannot be empty")
    
    # Verifica caracteres inválidos
    babel_alphabet = "abcdefghijklmnopqrstuvwxyz .,"
    invalid_chars = set(text) - set(babel_alphabet)
    if invalid_chars:
        raise ValidationError(
            f"Text contains invalid characters: {invalid_chars}. "
            f"Only allowed: {babel_alphabet}"
        )
    
    # Verifica tamanho
    if len(text) > MAX_SEARCH_LENGTH:
        raise TextTooLongError(
            f"Text too long: {len(text)} chars. "
            f"Babel limit is {MAX_SEARCH_LENGTH} chars. "
            f"Consider splitting into smaller chunks."
        )
    
    if verbose:
        print(f"[DEBUG] Text validation passed: {len(text)} chars")


# ==============================
# CORE FUNCTIONS - IMPROVED
# ==============================

def browse(
    hexagon: str,
    wall: str,
    shelf: str,
    volume: str,
    page: str = "1",
    verbose: bool = False
) -> Optional[str]:
    """
    Retrieve content from a specific location in the Library of Babel.
    """

    _validate_hexagon(hexagon)
    _validate_numeric_range(wall, 1, 4, "Wall")
    _validate_numeric_range(shelf, 1, 5, "Shelf")
    _validate_numeric_range(volume, 1, 32, "Volume")

    if not page.isdigit() or int(page) < 1:
        raise ValidationError("Page must be a positive integer")

    if verbose:
        print(f"[DEBUG] Browsing: hex={hexagon[:10]}..., wall={wall}, shelf={shelf}, vol={volume}, page={page}")

    try:
        response = _session.post(
            BROWSE_URL,
            data={
                "hex": hexagon,
                "wall": wall,
                "shelf": shelf,
                "volume": volume,
                "page": page
            },
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        
        if verbose:
            print(f"[DEBUG] Browse response status: {response.status_code}")
            
    except requests.Timeout as e:
        raise BrowseError(f"Request timed out after {REQUEST_TIMEOUT}s") from e
    except requests.ConnectionError as e:
        raise BrowseError(f"Connection failed: {e}") from e
    except requests.RequestException as e:
        raise BrowseError(f"Failed to retrieve page: {e}") from e

    soup = BeautifulSoup(response.text, "html.parser")
    pre_tag = soup.find("pre", id="textblock")

    if not pre_tag:
        if verbose:
            print("[DEBUG] No textblock found in response")
        return None

    content = pre_tag.get_text()
    
    if verbose:
        print(f"[DEBUG] Retrieved {len(content)} characters")
        
    return content


def search(
    book_text: str,
    verbose: bool = False,
    max_retries: int = 5
) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Search for text in the Library of Babel.
    Returns (hexagon, wall, shelf, volume, page)
    
    Improved version with:
    - Detailed error messages
    - Retry logic
    - Text validation
    - Better parsing
    """

    # Validação
    try:
        _validate_search_text(book_text, verbose=verbose)
    except TextTooLongError as e:
        raise SearchError(str(e)) from e

    if verbose:
        print(f"[DEBUG] Searching for text of length {len(book_text)}")
        print(f"[DEBUG] First 50 chars: {book_text[:50]}")

    last_error = None
    
    for attempt in range(max_retries):
        try:
            if verbose and attempt > 0:
                print(f"[DEBUG] Retry attempt {attempt + 1}/{max_retries}")
            
            response = _session.post(
                SEARCH_URL,
                data={"find": book_text},
                timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            
            if verbose:
                print(f"[DEBUG] Search response status: {response.status_code}")
                print(f"[DEBUG] Response length: {len(response.text)} chars")
            
            # Parse da resposta
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Debug: salvar HTML se não encontrar
            location_div = soup.find("div", class_="location")
            
            if not location_div:
                if verbose:
                    print("[DEBUG] No location div found")
                    # Verifica se há mensagem de erro específica
                    error_msg = soup.find("div", class_="error")
                    if error_msg:
                        print(f"[DEBUG] Error message from Babel: {error_msg.get_text()}")
                return None, None, None, None, None

            link = location_div.find("a", class_="intext")

            if not link or "onclick" not in link.attrs:
                if verbose:
                    print("[DEBUG] No link with onclick found")
                return None, None, None, None, None

            onclick_value = link["onclick"]
            
            if verbose:
                print(f"[DEBUG] onclick value: {onclick_value}")

            match = re.search(
                r"postform\('(.*?)','(.*?)','(.*?)','(.*?)','(.*?)'\)",
                onclick_value
            )

            if not match:
                if verbose:
                    print(f"[DEBUG] Regex didn't match onclick pattern")
                return None, None, None, None, None

            hexagon, wall, shelf, volume, page = match.groups()
            
            if verbose:
                print(f"[DEBUG] Found coordinates:")
                print(f"  Hexagon: {hexagon[:20]}...")
                print(f"  Wall: {wall}")
                print(f"  Shelf: {shelf}")
                print(f"  Volume: {volume}")
                print(f"  Page: {page}")

            return hexagon, wall, shelf, str(int(volume)), page
            
        except requests.Timeout as e:
            last_error = f"Request timed out after {REQUEST_TIMEOUT}s"
            if verbose:
                print(f"[DEBUG] {last_error}")
        except requests.ConnectionError as e:
            last_error = f"Connection failed: {str(e)[:100]}"
            if verbose:
                print(f"[DEBUG] {last_error}")
        except requests.RequestException as e:
            last_error = f"Request failed: {str(e)[:100]}"
            if verbose:
                print(f"[DEBUG] {last_error}")
        except Exception as e:
            last_error = f"Unexpected error: {str(e)[:100]}"
            if verbose:
                print(f"[DEBUG] {last_error}")
                import traceback
                traceback.print_exc()
        
        # Backoff exponencial entre tentativas
        if attempt < max_retries - 1:
            wait_time = (2 ** attempt) * 2  # 2s, 4s, 8s...
            if verbose:
                print(f"[DEBUG] Waiting {wait_time}s before retry...")
            time.sleep(wait_time)
    
    raise SearchError(f"Search failed after {max_retries} attempts. Last error: {last_error}")


def get_random_page(hexagon_length: int = HEXAGON_LENGTH) -> Optional[str]:
    """
    Retrieve a random page from the Library of Babel.
    """

    hexagon = "".join(
        random.choice(VALID_ALPHABET + VALID_NUMBERS)
        for _ in range(hexagon_length)
    )

    wall = str(random.randint(1, 4))
    shelf = str(random.randint(1, 5))
    volume = str(random.randint(1, 32))

    return browse(hexagon, wall, shelf, volume)


# ==============================
# DIAGNOSTIC TOOLS
# ==============================

def test_connection(verbose: bool = True) -> bool:
    """Testa a conexão com o Babel."""
    
    if verbose:
        print("Testing connection to Library of Babel...")
    
    try:
        response = requests.get(BASE_URL, timeout=10)
        if response.status_code == 200:
            if verbose:
                print("✅ Connection successful!")
            return True
        else:
            if verbose:
                print(f"⚠️ Unexpected status code: {response.status_code}")
            return False
    except Exception as e:
        if verbose:
            print(f"❌ Connection failed: {e}")
        return False


def diagnose_search_failure(encoded_text: str) -> dict:
    """Diagnostica por que uma busca pode ter falhado."""
    
    issues = {
        "text_length": len(encoded_text),
        "too_long": len(encoded_text) > MAX_SEARCH_LENGTH,
        "invalid_chars": [],
        "is_empty": not encoded_text,
    }
    
    babel_alphabet = "abcdefghijklmnopqrstuvwxyz .,"
    invalid = set(encoded_text) - set(babel_alphabet)
    if invalid:
        issues["invalid_chars"] = list(invalid)
    
    return issues


# ==============================
# CLI TEST
# ==============================

if __name__ == "__main__":
    print("=" * 60)
    print("BABEL CONNECTION DIAGNOSTIC")
    print("=" * 60)
    
    # Teste de conexão
    test_connection(verbose=True)
    
    print("\n" + "=" * 60)
    print("BABEL SEARCH TEST")
    print("=" * 60)
    
    test_text = "hello world"
    print(f"Searching for: '{test_text}'")
    
    try:
        result = search(test_text, verbose=True)
        
        if result[0]:
            hexagon, wall, shelf, volume, page = result
            print(f"\n✅ Found at:")
            print(f"   Hexagon: {hexagon[:20]}...")
            print(f"   Wall: {wall}")
            print(f"   Shelf: {shelf}")
            print(f"   Volume: {volume}")
            print(f"   Page: {page}")
            
            print("\nVerifying by browsing...")
            content = browse(hexagon, wall, shelf, volume, page, verbose=True)
            if content:
                print(f"Content preview: {content[:100]}")
            else:
                print("⚠️ Could not retrieve content")
        else:
            print("⚠️ Search returned no coordinates")
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
