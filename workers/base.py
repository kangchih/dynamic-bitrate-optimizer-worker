import datetime
import logging
from module import S3Provider
import os
import re
import subprocess
from functools import partial
from pathlib import Path
import posixpath
from .helper_func import download_video_from_url, minify_raw_video, get_video_info, get_video_info_cron
from utils import timer
from logging.handlers import TimedRotatingFileHandler
import face_recognition


class VmafBase:

    def __init__(self, mysql_api_host_write, mysql_api_host_read, mysql_api_port, mysql_api_user, mysql_api_pwd, mysql_api_db, aws_access_key_id,
                 aws_secret_access_key, s3_upload_bucket, s3_upload_key_prefix, video_download_dir, vmaf_threshold=90.0,
                 console_log_level="DEBUG", log_file=None, file_log_level="INFO",
                 max_video_height=960, max_video_width=960, max_fps=30.0, crf_start=26, crf_stop=33, crf_step=2,
                 ffmpeg_preset="slow", ffmpeg_preset_webp="default", log_interval=5, log_backup_count=20):
        """Lang Dynamic Bitrate Optimizer Base Class

        See also:
        CRF: https://trac.ffmpeg.org/wiki/Encode/H.264
        VMAF: https://github.com/Netflix/vmaf

        :param float vmaf_threshold: VMAF lower bound value for video compression
        :param str tag: compression tag, this tag will be prepended to the s3 upload key
        :param str video_download_dir: local path for temporarily storing videos
        :param str aws_access_key_id: aws access key
        :param str aws_secret_access_key: aws secret key
        :param str s3_upload_bucket: bucket for uploading compressed videos
        :param str s3_upload_key_prefix: key prefix of the uploaded videos
        :param str log_file: file path of the error log file
        :param int max_video_height: maximum value of the output height
        :param int max_video_width: maximum value of the output width
        :param float max_fps: maximum value of the output fps
        :param str console_log_level: [ERROR|WARNING|INFO|DEBUG]
        :param str file_log_level: [ERROR|INFO]
        :param int crf_start: starting crf of dbo testing
        :param int crf_stop: stopping crf of dbo testing
        :param int crf_step: crf step of dbo testing
        :param str ffmpeg_preset: [veryslow|slower|slow|default] ffmpeg preset for dbo testing
        :param str ffmpeg_preset_webp: [photo|picture|drawing|icon|default] ffmpeg preset for webp testing
        """
        # log folder under /app. Ex: /app/log/logfile
        self.log_path = Path('./log')
        self.log_path.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(console_log_level)
        self.logger.debug(f"[VmafBase][init] Console log level: {console_log_level}, File log level: {file_log_level}")

        log_handler = TimedRotatingFileHandler(posixpath.join(self.log_path, log_file),
                                               when="m",
                                               interval=log_interval,
                                               backupCount=log_backup_count)
        log_handler.setLevel(file_log_level)
        formatter = logging.Formatter('%(message)s')
        log_handler.setFormatter(formatter)
        self.logger.addHandler(log_handler)
        self.vmaf_threshold = vmaf_threshold
        self.video_download_path = Path(video_download_dir)
        self.video_download_path.mkdir(parents=True, exist_ok=True)
        self.max_video_height = max_video_height
        self.max_video_width = max_video_width
        self.max_fps = max_fps
        self.download_video_from_url = partial(download_video_from_url, logger=self.logger)
        self.s3_upload_bucket = s3_upload_bucket
        self.s3_upload_key_prefix = s3_upload_key_prefix.strip("/")
        self.minify_raw_video = partial(minify_raw_video, logger=self.logger)
        self.get_video_info = partial(get_video_info, logger=self.logger)
        self.get_video_info_cron = partial(get_video_info_cron, logger=self.logger)

        self.write_db_config = {'host': mysql_api_host_write, 'port': mysql_api_port, 'user': mysql_api_user,
                                'password': mysql_api_pwd,
                                'database': mysql_api_db, 'charset': "utf8mb4"}

        self.read_db_config = {'host': mysql_api_host_read, 'port': mysql_api_port, 'user': mysql_api_user,
                               'password': mysql_api_pwd,
                               'database': mysql_api_db, 'charset': "utf8mb4"}

        self.s3 = S3Provider.S3(aws_access_key_id, aws_secret_access_key)

        # CRFparameters
        if crf_start > crf_stop:
            raise ValueError(f"[VmafBase][init] crf_start should be <= crf_stop ({crf_start}, {crf_stop})")
        self.crf_start = crf_start
        self.crf_stop = crf_stop
        if crf_step < 1:
            raise ValueError(f"[VmafBase][init] crf_step should be >= 1 ({crf_step})")
        self.crf_step = crf_step

        # Don't use faster preset
        valid_presets = ["veryslow", "slower", "slow"]
        if ffmpeg_preset not in valid_presets:
            raise ValueError(f"[VmafBase][init] ffmpeg_preset should be one of {valid_presets} ({ffmpeg_preset})")
        self.ffmpeg_preset = ffmpeg_preset
        self.ffmpeg_preset_webp = ffmpeg_preset_webp

    @timer
    def compression_with_vmaf_threshold(self, mp4_file, face_detected=False):
        self.logger.debug(f"[compression_with_vmaf_threshold] mp4_file:{mp4_file}")
        best_compressed = None
        best_crf = 0
        vmaf = -1
        vmaf_threshold = self.vmaf_threshold
        crf_start = self.crf_start
        crf_stop = self.crf_stop

        if face_detected:
            crf_start = 24
            crf_stop = 29
            vmaf_threshold = 93

        self.logger.debug(f"[compression_with_vmaf_threshold] face_detected:{face_detected}, crf_start:{crf_start}, crf_stop:{crf_stop}, vmaf_threshold:{vmaf_threshold}")

        # TODO: range could be configurable
        for crf in range(crf_start, crf_stop, self.crf_step):
            compressed = re.sub(".mp4", f".{crf}.mp4", mp4_file)

            # compress with CRF value
            compression_cmd = f"ffmpeg -y -i {mp4_file} -loglevel panic -c:v libx264 -preset {self.ffmpeg_preset} -crf {crf} -c:a copy {compressed}"
            self.logger.debug(f"[compression_with_vmaf_threshold] Compress {mp4_file} with crf= {crf} with command= {compression_cmd}")
            subprocess.call(compression_cmd.split(" "))
            if not os.path.isfile(compressed):
                continue

            # check VMAF
            self.logger.debug(f"[compression_with_vmaf_threshold] Check VMAF for compressed= {compressed}")
            libvmaf_cmd = f"ffmpeg -i {compressed} -i {mp4_file} -loglevel panic -lavfi libvmaf -f null -"
            self.logger.debug(f"[compression_with_vmaf_threshold] libvmaf_cmd= {libvmaf_cmd}")

            libvmaf_result = subprocess.check_output(libvmaf_cmd.split(" ")).decode("utf-8").strip()
            self.logger.debug(f"[compression_with_vmaf_threshold] libvmaf_result= {libvmaf_result}")

            # response example: 'Start calculating VMAF score...\nExec FPS: 34.285857\nVMAF score = 95.505113\n'
            vmaf = float(re.split("\s+", libvmaf_result)[-1])
            self.logger.debug(f"[compression_with_vmaf_threshold] dbo score= {vmaf}")

            if vmaf < vmaf_threshold:
                self.logger.debug(f"[compression_with_vmaf_threshold] dbo={vmaf} < self.vmaf_threshold={vmaf_threshold}")
                break
            else:
                best_compressed = compressed
                best_crf = crf
        # best_crf = 0 means this video no changes
        return best_compressed, best_crf, vmaf


    def error_msg(self, id, error_msg, func_name, service_name='webp'):
        err_log = {
            "error_info": {
                "recording_id": id,
                "func_name": func_name,
                "service_name": service_name,
                "error_msg": error_msg
            }

        }
        return err_log

    @timer
    def face_detect(self, image_files):
        is_face_detected = False
        self.logger.debug(f"[face_detect] image_files:{image_files}")

        for image_file in image_files:
            self.logger.debug(f"[face_detect] image_file:{image_file}")
            try:
                image = face_recognition.load_image_file(image_file)
                face_locations = face_recognition.face_locations(image)
                self.logger.debug(f"[face_detect] face_locations:{face_locations}")
                if (len(face_locations) > 0):
                    self.logger.debug(f"[face_detect] face detected in image_file:{image_file}")
                    is_face_detected = True
                    break
            except FileNotFoundError as e:
                self.logger.debug(f"[face_detect] image_file:{image_file} load image error:{e}")
                continue
        self.logger.debug(f"is_face_detected:{is_face_detected}")
        return is_face_detected


    #TODO: Use cv2 to resize image then detect it would speed up whole process
    @timer
    def process_mp4_to_jpg(self, recording_id, mp4_file, run_path, file_log):
        result = []
        """"
        Example path:
        video_cache/123456789012345678/detectface015.jpg
        """
        self.logger.debug(f"[process_mp4_to_jpg][{recording_id}] mp4_file={mp4_file}, run_path={run_path}")
        # output_file = mp4_file.replace(f'{recording_id}.mp4', output)
        cmd = f'ffmpeg -y -i {mp4_file} -loglevel panic -vf fps=1 {run_path}/detectface%03d.jpg'
        self.logger.debug(f"[process_mp4_to_jpg] Process {mp4_file} with command= {cmd}")
        res = subprocess.call(cmd, shell=True)
        file_log["procedures"]["process_mp4_to_jpg_cmd_result"] = {"cmd": cmd, "result": res}
        self.logger.debug(f"[procedures] res = {res}")
        for file in os.listdir(run_path):
            if file.startswith("detectface"):
                self.logger.debug(f"[process_mp4_to_jpg] {os.path.join(run_path, file)}")
                result.append(os.path.join(run_path, file))
        return result, file_log
