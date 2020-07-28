import os
import kombu
from kombu.mixins import ConsumerMixin
from workers.base import VmafBase
import datetime
import time
import json
import shutil
from module import DBoperation
from utils import get_rbmq_urls, send_err_email
import hashlib


class VmafWorker(VmafBase, ConsumerMixin):

    def __init__(self, mysql_api_host_write, mysql_api_host_read, mysql_api_port, mysql_api_user, mysql_api_pwd, mysql_api_db, tag, aws_access_key_id,
                 aws_secret_access_key,
                 s3_upload_bucket, s3_upload_key_prefix, rbmq_hosts, rbmq_port, rbmq_user, rbmq_pwd, rbmq_vhost, inbound_queue,
                 inbound_routing_key, video_download_dir, vmaf_threshold, console_log_level="DEBUG",
                 file_log_level="INFO", log_file=None, s3_download_bucket=None, s3_download_key_prefix=None,
                 max_video_height=960, max_video_width=960, max_fps=30, crf_start=26, crf_stop=33, crf_step=2,
                 static_vframes=1, dynamic_vframes=9, lossless=1, jpg_w_size='1334', jpg_h_size='750',
                 webp_w_size='667', webp_h_size='375', webp_w_small_size='333', webp_h_small_size='187', webp_loop=0,
                 webp_fps=10, ffmpeg_preset='slow', ffmpeg_preset_webp='default', log_interval=5, log_backup_count=20,
                 clean_folder='True', smart_download='False'):

        (rbmq_url, rbmq_alt_urls) = get_rbmq_urls(rbmq_hosts, rbmq_port, rbmq_user, rbmq_pwd)
        self.rbmq_url = rbmq_url
        self.rbmq_vhost = rbmq_vhost
        self.rbmq_alt_urls = rbmq_alt_urls
        self.connection = kombu.Connection(self.rbmq_url, alternates=self.rbmq_alt_urls, failover_strategy='round-robin', virtual_host=self.rbmq_vhost)
        self.inbound_queue = kombu.Queue(name=inbound_queue, routing_key=inbound_routing_key, channel=self.connection, auto_declare=False)
        self.clean_folder = clean_folder
        self.smart_download = smart_download

        self.tag = tag
        super().__init__(mysql_api_host_write=mysql_api_host_write, mysql_api_host_read=mysql_api_host_read,
                         mysql_api_port=mysql_api_port, mysql_api_user=mysql_api_user, mysql_api_pwd=mysql_api_pwd,
                         mysql_api_db=mysql_api_db, aws_access_key_id=aws_access_key_id,
                         aws_secret_access_key=aws_secret_access_key, s3_upload_bucket=s3_upload_bucket,
                         s3_upload_key_prefix=s3_upload_key_prefix, video_download_dir=video_download_dir,
                         vmaf_threshold=vmaf_threshold, console_log_level=console_log_level, log_file=log_file,
                         file_log_level=file_log_level, max_video_height=max_video_height,
                         max_video_width=max_video_width, max_fps=max_fps, crf_start=crf_start, crf_stop=crf_stop,
                         crf_step=crf_step, ffmpeg_preset=ffmpeg_preset, ffmpeg_preset_webp=ffmpeg_preset_webp,
                         log_interval=log_interval, log_backup_count=log_backup_count)
        self.logger.debug(f"[VmafWorker] rbmq_url={self.rbmq_url}, rbmq_alt_urls={self.rbmq_alt_urls}")
        # Setup download mode
        #TODO: Refactor http download mode
        if s3_download_key_prefix is not None and s3_download_bucket is not None:
            self.download_mode = "s3"
        else:
            s3_download_bucket, s3_download_key_prefix = s3_upload_bucket, s3_upload_key_prefix
            self.download_mode = "s3"

        # self.video_upload_name = hashlib.md5(str(time.time()).encode('utf-8')).hexdigest() +'.mp4'
        self.s3_download_bucket = s3_download_bucket
        self.static_vframes = static_vframes
        self.dynamic_vframes = dynamic_vframes
        self.lossless = lossless
        self.jpg_w_size = jpg_w_size
        self.jpg_h_size = jpg_h_size
        self.webp_w_size = webp_w_size
        self.webp_h_size = webp_h_size
        self.webp_w_small_size = webp_w_small_size
        self.webp_h_small_size = webp_h_small_size
        self.webp_loop = webp_loop
        self.webp_fps = webp_fps

    def get_consumers(self, Consumer, channel):
        return [
            Consumer(queues=self.inbound_queue,
                     callbacks=[self.on_message],
                     accept=None,
                     prefetch_count=1)]

    def on_message(self, body, message):
        self.logger.debug(f"[DboWorker][on_message] RECEIVED MESSAGE: {message}")
        self.logger.debug(f"[DboWorker][on_message] RECEIVED MESSAGE PAYLOAD: {message.payload}")
        try:
            recording_id = None
            push = False
            if type(message.payload) is str:
                payload = json.loads(message.payload.replace("'", '"'))
            elif type(message.payload) is dict:
                payload = message.payload
            else:
                raise ValueError()

            self.logger.debug(f"process payload: {payload}")

            on_message_log = {
                "on_message_log": payload,
                "queue_type": "dbo_queue"
            }
            self.logger.info(json.dumps(on_message_log))
            file_log = {
                "dbo_info": {
                    "type": "dbo_conversion_log",
                    "worker_type": "dbo_queue",
                    "compression_tag": self.tag,
                },
                "time_start": str(datetime.datetime.utcnow()),
                "message_from_queue": dict(),
                "procedures": dict()
            }

            if("recording_id" in payload):
                recording_id = str(payload["recording_id"])
                self.logger.debug(f"[DboWorker][on_message] recording_id={recording_id}")
                file_log["message_from_queue"]["recording_id"] = recording_id
            else:
                self.logger.debug(f"[DboWorker][on_message] recording_id key error")
                file_log["message_from_queue"]["recording_id"] = recording_id

            if("time" in payload):
                time = payload["time"]
                self.logger.debug(f"[DboWorker][on_message] time={time}")
                file_log["message_from_queue"]["time"] = time
            else:
                self.logger.debug(f"[DboWorker][on_message] time key error")
                # Msg doesn't provide timestamp info: set to -1
                file_log["message_from_queue"]["time"] = -1

            # There is a cron job trying to re-submit failed recording_id to  'video_upload_complete' queue
            # The queue msg will have 'push' key in payload
            if ("push" in payload):
                push = payload["push"]
                self.logger.debug(f"[DboWorker][on_message] key exists: 'push'={push}'")

                file_log["message_from_queue"]["push"] = str(push)
            else:
                self.logger.debug(f"[DboWorker][on_message] push key error")
                file_log["message_from_queue"]["push"] = 'None'

            long_video = 0
            fields = ['url', 'long_url']
            url = DBoperation.get_values_from_db(table='recording', id=recording_id, fields=fields,
                                                 config=self.read_db_config, logger=self.logger)
            self.logger.debug(f"[process_recording][{recording_id}] url={url}")

            if (url[1] is None or url[1] is ''):
                self.logger.debug(f"[process_recording][{recording_id}] There is no long_url!")
            else:
                long_video = 1

            # Dump log
            self.logger.info(json.dumps(file_log))
            if ((recording_id is not None) and (push)):
                self.logger.debug(f"[DboWorker][on_message] long_video={long_video}")
                for lv in range(long_video+1):
                    success = self.process_recording(recording_id, file_log, lv)
                    if (not success):
                        self.logger.debug(f"[DboWorker][on_message] send alert email!!")
                        send_err_email(recording_id)
                    else:
                        self.logger.debug(f"[DboWorker][on_message] lv={lv} Done.")

        except Exception as e:
            self.logger.debug(f"[DboWorker][on_message] error={e}")
            self.logger.error(json.dumps(self.error_msg(recording_id, str(e), 'on_message', "dbo")))

        finally:
            self.logger.debug(f"[DboWorker][on_message] msg acked!!!!!!")
            message.ack()


    def process_recording(self, recording_id, file_log, long_video=0):
        run_path = None
        upload_mp4_file = None
        tic = time.time()
        success = False

        try:
            # setup run_path
            run_path = self.video_download_path.joinpath(recording_id)
            run_path.mkdir(parents=True, exist_ok=True)
            self.logger.debug(f"[process_recording][{recording_id}] Run path: {run_path}, long_video = {long_video}")

            # download video
            mp4_file = str(run_path.joinpath(f"{recording_id}.mp4"))

            self.logger.debug(f"[process_recording][{recording_id}] mp4_file={mp4_file}")
            file_log["procedures"]["long_video"] = str(long_video)
            file_log["procedures"]["run_path"] = str(run_path)
            file_log["procedures"]["mp4_file"] = {"mp4_file": mp4_file, "exist": os.path.exists(mp4_file)}

            try:
                # If mp4_file exists, no need to download again
                # NOTE: If smart_download = False => s3 download will be triggered anyway
                if((not os.path.exists(mp4_file)) or (self.smart_download == 'False')):
                    # Get url by recording_id
                    fields = ['url']
                    if (long_video == 1):
                        fields = ['long_url']
                    url = DBoperation.get_values_from_db(table='recording', id=recording_id, fields=fields, config=self.read_db_config, logger=self.logger)[0]
                    self.logger.debug(f"[process_recording][{recording_id}] url={url}")

                    if url is None:
                        raise ValueError(f"[process_recording][{recording_id}] url is None")
                    file_log["procedures"]["download_url"] = url
                    #TODO: 'download_mode == http' is not yet supported
                    if self.download_mode == "http":
                        _, td = self.download_video_from_url(local_file=mp4_file, url=url)
                    elif self.download_mode == "s3":
                        self.logger.debug(f"[process_recording][{recording_id}] download_url={url}")
                        #TODO: Remove comment after testing
                        _, td = self.s3.s3_download_file(bucket=self.s3_download_bucket, key=url, local_file=mp4_file, logger=self.logger)
                    else:
                        file_log["procedures"]["download"] = {"success": False, "dl": 'download failed'}
                        raise ValueError(f"[process_recording][{recording_id}] download mode is not set properly: {self.download_mode}")
                    file_log["procedures"]["download"] = {"success": True, "td": td}
                    self.logger.debug(f"[process_recording][{recording_id}] download_mode={self.download_mode}")
                else:
                    file_log["procedures"]["download_mode"] = self.download_mode

            except Exception as e:
                self.logger.debug(f"[process_recording][{recording_id}] download error={e}")
                file_log["procedures"]["download"] = {"success": False, "exception": str(e)}
                self.logger.error(json.dumps(self.error_msg(recording_id, str(e), 'process_recording', "dbo")))
                raise e

            # TODO: Check log
            self.logger.info(json.dumps(file_log))

            # Process mp4 to jpgs for face detect
            (jpgs, file_log), td = self.process_mp4_to_jpg(recording_id=recording_id, mp4_file=mp4_file, run_path=run_path, file_log=file_log)
            file_log["procedures"]["process_mp4_to_jpg_cmd_result"] = {"td": td, "number_of_jpgs": len(jpgs)}

            face_detected, td = self.face_detect(image_files=jpgs)
            file_log["procedures"]["face_detect"] = {"face_detected": face_detected, "td": td}
            self.logger.debug(f"[process_recording][{recording_id}] face_detected:{face_detected}, jpgs:{jpgs}")

            if (long_video == 0):
                # minify raw mp4 before compression if required
                video_info = self.get_video_info(mp4_file, dimensions=True, fps=True)
                self.logger.debug(f"[process_recording][{recording_id}] Video info: {video_info}")
                width, height, fps = video_info["width"], video_info["height"], video_info["r_frame_rate"]
                file_log["procedures"]["video_info"] = {"width": width, "height": height, "fps": fps}
                minify_ops = {}
                # check dimensions
                if width <= height and self.max_video_height < height:
                    # -2 see also: https://stackoverflow.com/questions/20847674/ffmpeg-libx264-height-not-divisible-by-2
                    minify_ops["dimensions"] = f"-2:{self.max_video_height}"
                elif height <= width and self.max_video_width < width:
                    minify_ops["dimensions"] = f"{self.max_video_width}:-2"
                # check fps
                if self.max_fps < fps:
                    minify_ops["fps"] = self.max_fps
                # TODO: Run crf 18 for android video from original cam
                """ffmpeg -i 288265560523836677.mp4 -c:v libx264 -preset veryslow -crf 18 -c:a copy 288265560523836677.18.mp4"""
                if minify_ops:
                    try:
                        (mp4_file, result), td = self.minify_raw_video(mp4_file, minify_ops)
                        #TODO: What if result != 0 (conversion failed)
                        if (result != 0):
                            file_log["procedures"]["minify"] = {"success": False, "td": td, "result": result}
                        upload_mp4_file = mp4_file
                        video_info_mini = self.get_video_info(upload_mp4_file, dimensions=True, fps=True)
                        self.logger.debug(f"[process_recording][{recording_id}] Video info mini: {video_info}")
                        width, height, fps = video_info_mini["width"], video_info_mini["height"], video_info_mini["r_frame_rate"]
                        file_log["procedures"]["video_info_mini"] = {"width": width, "height": height, "fps": fps}
                        file_log["procedures"]["minify"] = {"success": True, "td": td}
                    except Exception as e:
                        file_log["procedures"]["minify"] = {"success": False, "exception": str(e)}
                        self.logger.error(json.dumps(self.error_msg(recording_id, str(e), 'process_recording', "dbo")))
                        raise e
                else:
                    file_log["procedures"]["minify"] = {"ignore": True}

            # compress video
            try:
                (compressed_mp4_file, crf, vmaf_score), td = self.compression_with_vmaf_threshold(mp4_file, face_detected)
                self.logger.debug(f"file {mp4_file} is compressed with crf {crf} -> {compressed_mp4_file}")
                if compressed_mp4_file is not None:
                    self.logger.debug(f"[process_recording][{recording_id}] {compressed_mp4_file}")
                    upload_mp4_file = compressed_mp4_file
                file_log["procedures"]["compression"] = {"success": True, "crf": crf, "td": td, "compressed_mp4_file": compressed_mp4_file, "final_vmaf_score": vmaf_score}
            except Exception as e:
                file_log["procedures"]["compression"] = {"success": False, "exception": str(e)}
                self.logger.error(json.dumps(self.error_msg(recording_id, str(e), 'process_compression', "dbo")))
                if upload_mp4_file is None:
                    raise e

            self.logger.debug(f"[process_recording][{recording_id}] upload_mp4_file: {upload_mp4_file}, crf={crf}")

            if (upload_mp4_file is not None):
                # Check upload file size and upload video
                try:
                    upload_size = os.path.getsize(upload_mp4_file)
                    self.logger.debug(f"[process_recording][{recording_id}] upload_size={upload_size}")
                    if upload_size == 0:
                        e = ValueError("file size should be greater than 0")
                        file_log["procedures"]["sizecheck"] = {"success": False, "exception": str(e)}
                        self.logger.error(json.dumps(self.error_msg(recording_id, str(e), 'process_before_upload', "dbo")))
                        raise e
                    else:
                        file_log["procedures"]["sizecheck"] = {"success": True, "bytes": upload_size}

                    video_upload_name = hashlib.md5(str(time.time()).encode('utf-8')).hexdigest() + '.mp4'
                    self.logger.debug(f"[process_recording][{recording_id}] video_upload_name={video_upload_name}")
                    upload_key = f"{self.s3_upload_key_prefix}/{recording_id}/{self.tag}/{video_upload_name}"
                    if (long_video == 1):
                        upload_key = f"{self.s3_upload_key_prefix}/{recording_id}/{self.tag}/long/{video_upload_name}"
                    self.logger.debug(f"[process_recording][{recording_id}] upload_key={upload_key}")
                    #TODO: S3 test comment out
                    _, td = self.s3.s3_upload_file(bucket=self.s3_upload_bucket,
                                                   key=upload_key,
                                                   upload_file=upload_mp4_file,
                                                   logger=self.logger)
                    file_log["procedures"]["upload"] = {"success": True, "td": td, "upload_key": upload_key}
                    #TODO exception handler
                except Exception as e:
                    file_log["procedures"]["upload"] = {"success": False, "exception": str(e)}
                    self.logger.error(json.dumps(self.error_msg(recording_id, str(e), 'process_upload', "dbo")))
                    raise e

                # update db with new meta
                try:
                    if (long_video == 0):
                        update_dict = {'dbo_url': upload_key, 'width': width, 'height': height}
                    else:
                        update_dict = {'long_dbo_url': upload_key}

                    _ = DBoperation.update_db(table='recording', update_map=update_dict, id=recording_id,
                                              config=self.write_db_config, logger=self.logger)
                    file_log["procedures"]["update_recording"] = {"success": True}
                except Exception as e:
                    file_log["procedures"]["update_recording"] = {"success": False, "exception": str(e)}
                    self.logger.error(json.dumps(self.error_msg(recording_id, str(e), 'process_update_db', "dbo")))
                    raise e
            elif (long_video == 1):
                try:

                    update_dict = {'long_dbo_url': url}

                    _ = DBoperation.update_db(table='recording', update_map=update_dict, id=recording_id,
                                              config=self.write_db_config, logger=self.logger)
                    file_log["procedures"]["update_recording"] = {"success": True}
                except Exception as e:
                    file_log["procedures"]["update_recording"] = {"success": False, "exception": str(e)}
                    self.logger.error(json.dumps(self.error_msg(recording_id, str(e), 'process_update_db', "dbo")))
                    raise e

            else:
                file_log["procedures"]["upload"] = {"ignore": True}
                file_log["procedures"]["update_recording"] = {"ignore": True}
            file_log["dbo_info"]["complete"] = {"success": True}
            success = True

        except Exception as e:
            # don't let the worker stopped by exceptions
            file_log["dbo_info"]["complete"] = {"success": False, "exception": str(e)}
            self.logger.error(json.dumps(self.error_msg(recording_id, str(e), 'process_recording', "dbo")))
        finally:
            #TODO Err msg to queue
            # write file log, success => info,  failed => error
            toc = time.time()
            file_log["time_stop"] = str(datetime.datetime.utcnow())
            file_log["time_elapsed"] = f"{toc - tic:.3f}"
            self.logger.debug(f"[process_recording][{recording_id}] finally file_log:{file_log}")
            self.logger.debug(f"[process_recording][{recording_id}] success:{success}")
            self.logger.info(json.dumps(file_log))

            # cleanup local files
            if run_path is not None:
                self.logger.debug(f"[process_recording][{recording_id}]  Clean_folder={self.clean_folder}, Cleanup {run_path}")
                if (self.clean_folder == 'True'):
                    self.logger.debug(f"[process_recording][{recording_id}] Ready to cleanup {run_path}")
                    shutil.rmtree(str(run_path))
            return success