"""Legacy entry point kept as a compatibility forwarder.

The real execution chain lives in pipeline.aggregate; nothing else is retained
here so there is no alternative executor or conditional fallback.
"""
from pipeline import aggregate

__all__ = ["aggregate"]
