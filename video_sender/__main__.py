import argparse
import json
import time

import numpy as np

from .sender import VideoSender


def main():
  parser = argparse.ArgumentParser(description="Send a numbered synthetic pattern to PICO")
  parser.add_argument("--host", default="0.0.0.0")
  parser.add_argument("--port", type=int, default=13579)
  parser.add_argument("--width", type=int, default=928)
  parser.add_argument("--height", type=int, default=800)
  parser.add_argument("--fps", type=int, default=30)
  parser.add_argument("--bitrate", type=int, default=4000000)
  parser.add_argument("--encoder", choices=("h264_nvenc", "libx264"), default="h264_nvenc")
  args = parser.parse_args()
  sender = VideoSender(args.host, args.port, args.fps, args.bitrate, args.encoder)
  sender.start()
  index = 0
  try:
    while True:
      image = np.zeros((args.height, args.width, 3), dtype=np.uint8)
      image[:, :args.width // 2, 2] = 180
      image[:, args.width // 2:, 1] = 180
      image[:, index % args.width:(index % args.width) + 8] = 255
      sender.submit_frame(image)
      index += 8
      if index % 240 == 0:
        print(json.dumps(sender.snapshot()), flush=True)
      time.sleep(1 / args.fps)
  except KeyboardInterrupt:
    pass
  finally:
    sender.close()


if __name__ == "__main__":
  main()
