import os
import time
from email.utils import parsedate_to_datetime
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

MIN_REQUEST_INTERVAL = 1.0
MAX_ATTEMPTS = 3
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

_last_request_started = None


def retry_delay(retry_after, fallback):
    if not retry_after:
        return fallback

    try:
        return max(fallback, int(retry_after))
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(retry_after)
            return max(fallback, retry_at.timestamp() - time.time())
        except (ValueError, TypeError, OverflowError):
            return fallback


def open_wikimedia(request, *, timeout):
    global _last_request_started

    contact = os.environ.get("WIKIMEDIA_CONTACT") or (
        "https://github.com/Wanying0421/ece-big-data-imdb"
    )
    request.add_header("User-Agent", f"ECEIMDbEnrichment/0.1 ({contact})")

    for attempt in range(MAX_ATTEMPTS):
        if _last_request_started is not None:
            elapsed = time.monotonic() - _last_request_started
            time.sleep(max(0, MIN_REQUEST_INTERVAL - elapsed))
        _last_request_started = time.monotonic()

        delay = 2 ** attempt
        try:
            return urlopen(request, timeout=timeout)
        except HTTPError as error:
            if error.code not in RETRYABLE_STATUS or attempt == MAX_ATTEMPTS - 1:
                raise
            delay = retry_delay(error.headers.get("Retry-After"), delay)
            error.close()
        except (TimeoutError, URLError):
            if attempt == MAX_ATTEMPTS - 1:
                raise

        time.sleep(delay)