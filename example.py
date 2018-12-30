import tempfile
import signal
import sys
import time
import os
from pytranslive import Transcoder, TranscodeOptions

transcode_dir = "/tmp/transcode"

if not os.path.exists(transcode_dir):
    os.makedirs(transcode_dir)

transcoder = Transcoder()

options = TranscodeOptions()
options.selected_tracks = ["0:0", "0:1", "0:4"]
options.audio_codec = "aac"
options.stereo_downmix = True
options.width = 1280
#options.height = 800
options.video_bitrate = 8 * 1024 * 1024


options.format = "hls"

transcode_job = transcoder.get_transcode_job(transcode_dir, "stream.m3u8", options, "test.mkv")

def signal_handler(sig, frame):
        transcode_job.stop()
        sys.exit()

signal.signal(signal.SIGINT, signal_handler)

transcode_job.start()

while transcode_job.is_running:
    time.sleep(2)
    print("Progress time: " + str(transcode_job.progress_time) +", Speed: " + str(transcode_job.progress_speed) + "x")

print("Transcoding over !")