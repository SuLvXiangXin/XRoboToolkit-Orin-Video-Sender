"""Low-latency camera-to-PICO video gateway.

This package intentionally has no dependency on XRoboToolkit PC Service or any
robot/WBC control API.  Its only externally visible data planes are the
official Unity camera-control protocol and the encoded-video TCP stream.
"""

from .config import GatewayConfig
from .raw_source import LatestRawFrameQueue, RawFrameSource, RawVideoFrame

__all__ = ["GatewayConfig", "LatestRawFrameQueue", "RawFrameSource", "RawVideoFrame"]
