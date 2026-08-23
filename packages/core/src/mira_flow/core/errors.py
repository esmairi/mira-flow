"""Leaf module (like ../settings.py): one exception type shared by both the
consumer/ and producer/ packages, so callers can catch it uniformly instead
of two distinct classes with the same name.
"""

from __future__ import annotations


class CoreBrokerError(Exception):
    """Raised when a publish cannot be confirmed or a consumer fails fatally."""
