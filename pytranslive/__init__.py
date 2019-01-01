import subprocess
import shutil
import os
import signal
from threading import Thread
import sys
import time
import re
import json
import codecs

DEBUG = True

SUBS_HLS = '#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs",NAME="Default",URI="stream_out_vtt.m3u8",DEFAULT=YES,AUTOSELECT=YES,LANGUAGE="en"'

class TranscodeJob(object):

    def __init__(self, ffmpeg_cmd, output_dir, output_filename, options):
        self.ffmpeg_cmd = ffmpeg_cmd
        self.output_dir = output_dir
        self.output_filename = output_filename
        self.options = options
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

        # really ugly hack, to fix in ffmpeg
        if self.options.format == "hls":
            time.sleep(3)
            self.hls_postprocess()

    def handle_process_output(self):
        for line in codecs.getreader("utf-8")(self.process.stderr):
            elems = line.strip().split(" ")
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

    def hls_postprocess(self):
        with open(self.output_dir + os.sep + self.output_filename, 'r') as file:
            filedata = file.read()

        filedata = re.sub(r'(\#EXT\-X\-STREAM\-INF.*)', lambda o: '{}\n{},SUBTITLES=subs'.format(SUBS_HLS, o.groups(0)[0]), filedata)
        print(filedata)

        with open(self.output_dir + os.sep + self.output_filename, 'w') as file:
            file.write(filedata)

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

# AUTO = 0
# STREAM = 1
# BURNT = 2

class TranscodeOptions(object):

    def __init__(self):
        self.video_bitrate = 8388608 # 8mbps
        self.video_codec = "h264" # can be copy
        self.video_profile = None # optional, most common profile will be used in none case
        # if width or height is missing the AR will be used to calculate the missing one
        self.width = 1280
        self.height = None

        # self.subtitles_method = AUTO
        
        self.audio_bitrate = 131072 # 128kbps
        self.audio_codec = "aac" # can be copy
        self.audio_profile = None # optional, most common profile will be used in this case

        self.stereo_downmix = False

        self.time = None
        self.duration = None

        self.segment_duration = 3

        self.format = "hls"

        self.container = None

        # this assumes that track 0 & 1 are the one transcoded
        # you can use ffprobe to know the track numbers and specify here at most 3 tracks,
        # one for each stream type (video, audio, subtitles)
        self.selected_tracks = [0, 1]

    def sanitize(self):
        if not self.video_codec or not self.audio_codec:
            raise Exception('Output video and audio codecs mandatory, can be "copy"')

        self.video_codec = self.video_codec.strip().lower()
        if self.video_codec == "h265":
            self.video_codec = "hevc"
        if self.video_codec == "avc":
            self.video_codec = "h264"
        self.audio_codec = self.audio_codec.strip().lower()

        if self.format:
            self.format = self.format.strip().lower()

        if self.format == "hls":
            if not self.container or self.container == "fmp4" or self.container == "mp4":
                self.container = "fmp4"
            elif self.container == "mpegts" or self.container == "ts":
                self.container = "mpegts"
            else:
                raise Exception("HLS only supports TS and fMP4 containers")
        if not self.container:
            self.container = "matroska"
        else:
            self.container = self.container.strip().lower()
            if self.container in ["mkv", "webm"]:
                    self.container = "matroska"

class Transcoder(object):

    def __init__(self, hwaccel_device=None):
        # here we should autodectect hw capabilities:
        # - availability of hw decoders and encoders
        # - availability of hw filtering/overlay capabilities for subtitles and scaling

        self.hwaccel_type = None
        self.hwaccel_device = hwaccel_device

        self.hw_scalers = {
            "vaapi": "scale_vaapi",
        }

        # TODO probe ffmpeg -decoders
        self.libfdkaac_supported = False

        # self.tone_mapper = "opencl"

    def ffprobe(self, *input_urls):
        result = {}
        for url in input_urls:
            ffprobe_cmd = ["ffprobe", "-threads", "0", "-print_format", "json", "-show_streams", "-show_chapters", "-show_format"]
            ffprobe_cmd += [url.replace(" ", "%20")]
            try:
                res = subprocess.check_output(ffprobe_cmd)
                result[url] = json.loads(res)
            except Exception:
                pass
        return result

    def get_transcode_job(self, output_dir, output_filename="stream.m3u8", options=TranscodeOptions(), *input_urls):

        options.sanitize()

        self.probe = self.ffprobe(*input_urls)

        params = self.get_hwaccel_params(options, self.probe[input_urls[0]])

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

        params += self.get_timestamp_params(options)

        if options.video_codec == "copy":
            params += ["-codec:v", "copy"]
        else:
            params += self.get_video_encoder_params(options)
            params += self.get_video_filter_params(options)

        if options.audio_codec == "copy":
            params +=  ["-codec:a", "copy"]
        else:
            params += self.get_audio_params(options)

        params += self.get_subtitle_params(options)

        params += self.get_output_params(options, output_dir, output_filename)

        ffmpeg_cmd = ["ffmpeg"]
        for p in params:
            ffmpeg_cmd += [str(p)]

        return TranscodeJob(ffmpeg_cmd, output_dir, output_filename, options)

    def get_timestamp_params(self, options):
        return ["-max_delay", 5000000, "-copyts", "-start_at_zero"]

    def get_output_params(self, options, output_dir, output_filename):
        params = []

        if options.format == "dash":
            params += ["-f", "dash", "-window_size", 0, "-cluster_time_limit", options.segment_duration]
            params += ["-segment_list_flags", "+live"]
        elif options.format == "hls":
            params += ["-f", "hls", "-hls_segment_type", options.container, "-hls_playlist_type", "event", "-segment_list_type", "m3u8"]
            params += ["-segment_list_flags", "+live"]
            params += ["-segment_start_number", 0, "-segment_time", options.segment_duration, "-segment_time_delta", 0.05]
            params += ["-master_pl_name", output_filename]

            # the output filename is used for the master playlist,
            # let's add _out to the name for the main track
            dot = output_filename.rfind('.')
            name = output_filename[:dot]
            ext = output_filename[dot+1:]
            output_filename = "{}_out.{}".format(name, ext)
        else:
            params += ["-f", options.container]

        params += ["-y", output_dir + os.sep + output_filename]
        return params

    def get_audio_params(self, options):
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

        if options.stereo_downmix:
            params += ["-ac", 2, "-clev", "3dB", "-slev", "-3dB"]

        return params

    def get_subtitle_params(self, options):
        # TODO
        return ["-codec:s", "webvtt"]

    def get_hw_decoder_type(self, codec, profile):
        # TODO probe using vainfo
        if codec == "h264" or codec == "hevc" or codec == "vp9":
            return "vaapi"
        return None

    def get_hw_encoder(self, options):
        # TODO probe using vainfo
        hw_type = "vaapi"
        if options.video_codec == "h264" or options.video_codec == "hevc" or options.video_codec == "vp9":
            return "{}_{}".format(options.video_codec, hw_type)
        return None

    def get_video_encoder(self, options):
        hw_encoder = self.get_hw_encoder(options)
        if hw_encoder:
            return hw_encoder

        if options.video_codec == "h264":
            return "libx264"

        if options.video_codec == "hevc":
            return "libx265"

        if options.video_codec == "vp9":
            return "libvpx"

        return None

    def get_hwaccel_params(self, options, probe):
        # find codec & profile of main video track to probe hw decoder support
        for s in probe['streams']:
            # TODO deals with multiple video streams
            if s['codec_type'] == "video":
                codec = s['codec_name']
                profile = s['profile']
                hw_type = self.get_hw_decoder_type(codec, profile)
                if hw_type:
                    self.hwaccel_type = hw_type

        params = []
        if self.hwaccel_type:
            init_hw_device = "{0}={0}".format(self.hwaccel_type)
            if self.hwaccel_device:
                init_hw_device += ":" + self.hwaccel_device
            params += ["-init_hw_device", init_hw_device]
            params += ["-hwaccel", self.hwaccel_type, "-hwaccel_output_format", self.hwaccel_type, "-hwaccel_device", self.hwaccel_type, "-filter_hw_device", self.hwaccel_type]
        return params

    def get_video_encoder_params(self, options):
        params = ["-codec:v", self.get_video_encoder(options)]

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
        if not self.hwaccel_type and self.get_hw_encoder(options):
            vf_str = "format=nv12|" + self.hwaccel_type + ",hwupload,"
        
        vf_str += self.get_scaler_filter(options)

        vf_str += self.get_subtitle_filter(options)


        #vf_str = "[0:v]scale_vaapi=1920:800,hwdownload,format=nv12[base];[0:s]scale=1920:800[subtitle];[base][subtitle]overlay[v];[v]hwupload[v]"

        # example to render srt subs using an overlay filter, should be useful once I get hw overlay working on 4.1
        # vf_str = "[0:v]scale_vaapi=1920:800,hwdownload,format=nv12[base];color=color=#00000000:size=1920x800,subtitles=test.mkv:si=1:alpha=1[subtitle];[base][subtitle]overlay[v];[v]hwupload[v]"
        # "-map", "[v]"


        if vf_str[-1] == ",":
            vf_str = vf_str[:-1]

        return ["-vf", vf_str]


    def get_scaler_filter(self, options):
        if not options.width and not options.height:
            return ""

        if options.width and (not options.height or options.height < 0):
            scale_str = "trunc(min(max(iw\,ih*dar)\,{})/2)*2:trunc(ow/dar/2)*2".format(options.width)
        else:
            # TODO handle when height only specified
            scale_str = "{}:{}".format(options.width, options.height)

        scaler = "scaler"
        if self.hwaccel_type in self.hw_scalers:
            scaler = self.hw_scalers[self.hwaccel_type]

        return "{}={},".format(scaler, scale_str)

    def get_subtitle_filter(self, options):
        return ""

        #return "hwdownload,format=nv12,subtitles=test.mkv:si=1,hwupload"
        #return "hwdownload,format=nv12,[0:s]overlay,hwupload[v]"