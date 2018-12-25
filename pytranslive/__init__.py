import subprocess
import shutil
import os
import signal
from threading import Thread
import sys
import time

DEBUG = True

class TranscodeJob(object):

    def __init__(self, ffmpeg_cmd, output_dir):
        self.ffmpeg_cmd = ffmpeg_cmd
        self.output_dir = output_dir
        self.process = None
        self.progress_time = None
        self.progress_speed = None
        self.is_running = False

    def start(self):
        self.is_running = True
        if DEBUG:
            print(self.ffmpeg_cmd)
        self.process = subprocess.Popen(self.ffmpeg_cmd, stderr=subprocess.PIPE)

        Thread(target=self.handle_process_output).start()

    def handle_process_output(self):
        for line in iter(self.process.stderr.readline, b''):
            if DEBUG:
                sys.stderr.write(line)
            elems = str(line).split(" ")
            for elem in elems:
                elem = elem.strip()
                if elem.startswith("time="):
                    self.progress_time = elem[5:]
                if elem.startswith("speed="):
                    s = elem[6:-1]
                    if s:
                        self.progress_speed = float(s)

        # wait for the process to end
        self.process.communicate()
        self.is_running = False
        self.process = None

    def stop(self):
        if self.process:
            try:
                os.kill(self.process.pid, signal.SIGTERM)
                time.sleep(1)
                r = os.waitpid(self.process.pid, os.WNOHANG)
                # process still not over, let's wait a bit and SIGKILL it
                if r[0] == 0:
                    time.sleep(2)
                    os.kill(self.process.pid, signal.SIGKILL)
            except Exception:
                pass

    def delete(self):
        self.stop()
        shutil.rmtree(self.output_dir)

class TranscodeOptions(object):

    def __init__(self):
        self.video_bitrate = 8388608 # 8mbps
        self.video_codec = "h264" # can be copy
        self.video_profile = None # optional, most common profile will be used in this case
        # if width or height is missing the AR will be used to calculate the missing one
        self.width = 1280
        self.height = None
        
        self.audio_bitrate = 131072 # 128kbps
        self.audio_codec = "aac" # can be copy
        self.audio_profile = None # optional, most common profile will be used in this case

        self.time = None
        self.duration = None

        self.segment_duration = 3

        self.format = "hls"

        # this assumes that track 0 & 1 are the one transcoded
        # you can use ffprobe to know the track numbers and specify here at most 3 tracks,
        # one for each stream type (video, audio, subtitles)
        self.selected_tracks = [0, 1]

class Transcoder(object):

    def __init__(self, hwaccel_type="vaapi", hwaccel_device=None):
        # here we should autodectect hw capabilities:
        # - availability of hw decoders and encoders
        # - availability of hw filtering/overlay capabilities for subtitles and scaling
        
        self.encoders = {}
        self.encoders["h264"] = ["libx264"]
        self.encoders["hevc"] = ["libx265"]

        if hwaccel_type:
            self.encoders["h264"].insert(0, "h264_" + hwaccel_type)
            self.encoders["hevc"].insert(0, "hevc_" + hwaccel_type)

        self.hwaccel_type = hwaccel_type
        self.hwaccel_device = hwaccel_device

        self.hw_scalers = {
            "vaapi": "scale_vaapi",
        }

        # TODO probe ffmpeg -decoders
        self.libfdkaac_supported = False

        # self.tone_mapper = "opencl"

    def get_transcode_job(self, output_dir, output_filename="stream.m3u8", options=TranscodeOptions(), *input_urls):

        params = self.get_hwaccel_params(options)

        for url in input_urls:
            url.replace(" ", "%20")
            params += ["-i", url]

        for track in options.selected_tracks:
            track = str(track)
            if not ":" in track:
                "0:" + track
            params += ["-map", track]

        params += ["-threads", 0, "-map_metadata", -1, "-map_chapters", -1]

        if options.time:
            params += ["-ss", options.time]

        params += self.get_output_format_params(options)
        params += self.get_timestamp_params(options)

        params += self.get_video_encoder_params(options)
        params += self.get_video_filter_params(options)

        params += self.get_audio_params(options)

        params += self.get_subtitle_params(options)

        params += ["-segment_start_number", 0]
        params += ["-y", output_dir + os.sep + output_filename]

        ffmpeg_cmd = ["ffmpeg"]
        for p in params:
            ffmpeg_cmd += [str(p)]

        return TranscodeJob(ffmpeg_cmd, output_dir)

    def get_timestamp_params(self, options):
        return ["-max_delay", 5000000, "-avoid_negative_ts", "disabled", "-copyts", "-start_at_zero"]

    def get_output_format_params(self, options):
        # TODO DASH support
        return ["-f", "hls", "-segment_list_flags", "+live", "-hls_segment_type", "fmp4", "-hls_list_size", 0, "-segment_list_type", "m3u8", "-segment_time", options.segment_duration]

    def get_audio_params(self, options):
        if options.audio_codec == "copy" or not options.audio_codec:
            return ["-codec:a", "copy"]
        else:
            params = []

            params += ["-b:a", options.audio_bitrate]

            codec = options.audio_codec
            if codec == "aac" and self.libfdkaac_supported:
                codec = "libfdkaac"

            params += ["-codec:a", codec]

            profile = options.audio_profile
            # ffmpeg internal aac encoder only supports LC profile
            if profile and codec == "aac" and not self.libfdkaac_supported:
                    profile = None

            if profile:
                params += ["-profile:a", profile]

            # TODO channels

            return params


    def get_subtitle_params(self, options):
        # TODO
        return ["-codec:s", "webvtt"]

    def get_hwaccel_params(self, options):
        params = []
        if self.hwaccel_type:
            init_hw_device = self.hwaccel_type + "=" + self.hwaccel_type
            if self.hwaccel_device:
                init_hw_device += ":" + self.hwaccel_device
            params += ["-init_hw_device", init_hw_device]
            params += ["-hwaccel", self.hwaccel_type, "-hwaccel_output_format", self.hwaccel_type, "-hwaccel_device", self.hwaccel_type, "-filter_hw_device", self.hwaccel_type]
        return params

    def get_video_encoder_params(self, options):
        encoder = None
        if options.video_codec in self.encoders:
            encoder = self.encoders[options.video_codec][0]
        
        params = ["-codec:v", encoder]
        # target 90% of the specified bitrate and limit the bitrate to the one specified
        # it avoids a CBR stream and allows to be a bit under the targeted bitrate,
        # it should helps some cases where the client bandwidth is fluctuating
        params += ["-b:v", int(options.video_bitrate*0.9)]
        params += ["-maxrate", options.video_bitrate]
        params += ["-bufsize", options.video_bitrate*2]

        if options.video_profile:
            params += ["-profile:v", options.video_profile]

        # force keyframe at each segment boundary
        params += ["-force_key_frames", "expr:if(isnan(prev_forced_t),eq(t,t),gte(t,prev_forced_t+" + str(options.segment_duration) + "))"]

        return params

    def get_video_filter_params(self, options):
        vf_str = ""

        # In case the input is not HW decodable we need to upload the surfaces
        # to the HW encoder. This does nothing when the HW decoder is used
        if self.hwaccel_type:
            vf_str = "format=nv12|" + self.hwaccel_type + ",hwupload,"
        
        vf_str += self.get_scaler_filter(options)

        return ["-vf", vf_str]

    def get_scaler_filter(self, options):
        if not options.width and not options.height:
            return None

        vf_str = ""
        
        width = options.width
        height = options.height
        # we use -2 to keep it a multiple of 2
        if not width and height > 0:
            width = -2
        if not height and width > 0:
            height = -2

        scaler = "scaler"
        if self.hwaccel_type in self.hw_scalers:
            scaler = self.hw_scalers[self.hwaccel_type]
        
        return scaler + "=" + str(width) + ":" + str(height)
