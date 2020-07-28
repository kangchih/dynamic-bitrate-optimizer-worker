import os
from kombu.mixins import ConsumerProducerMixin
import kombu
from utils import timer, get_rbmq_urls
from workers.base import VmafBase
import datetime
import time
import json
import shutil
from module import DBoperation
import subprocess


class WebpWorker(VmafBase, ConsumerProducerMixin):

    def __init__(self, mysql_api_host_write, mysql_api_host_read, mysql_api_port, mysql_api_user, mysql_api_pwd,
                 mysql_api_db, aws_access_key_id, aws_secret_access_key, s3_upload_bucket, s3_upload_key_prefix,
                 s3_upload_song_key_prefix, rbmq_hosts, rbmq_port, rbmq_user, rbmq_pwd, rbmq_vhost,
                 inbound_queue, inbound_routing_key, video_download_dir, console_log_level="DEBUG",
                 file_log_level="INFO", log_file=None, lossless=1, s3_download_bucket=None, s3_download_key_prefix=None,
                 static_vframes=1, dynamic_vframes=9, jpg_w_size='1334', jpg_h_size='750', webp_w_size='667',
                 webp_h_size='375', webp_w_small_size='333', webp_h_small_size='187', webp_loop=0, webp_fps=10,
                 audio_bitrate=19200, ffmpeg_preset_webp='default', log_interval=5, log_backup_count=20,
                 clean_folder='True'):

        (rbmq_url, rbmq_alt_urls) = get_rbmq_urls(rbmq_hosts, rbmq_port, rbmq_user, rbmq_pwd)
        self.rbmq_url = rbmq_url
        self.rbmq_vhost = rbmq_vhost
        self.rbmq_alt_urls = rbmq_alt_urls
        self.connection = kombu.Connection(self.rbmq_url, alternates=self.rbmq_alt_urls, failover_strategy='round-robin', virtual_host=self.rbmq_vhost)
        # self.exchange = kombu.Exchange(exchange, type=exchange_type)
        self.inbound_queue = kombu.Queue(name=inbound_queue, routing_key=inbound_routing_key, channel=self.connection, auto_declare=False)
        # self.outbound_queue = kombu.Queue(name=outbound_queue, exchange=self.exchange, routing_key=outbound_routing_key, channel=self.connection, auto_declare=False)
        self.clean_folder = clean_folder
        super().__init__(mysql_api_host_write=mysql_api_host_write, mysql_api_host_read=mysql_api_host_read,
                         mysql_api_port=mysql_api_port, mysql_api_user=mysql_api_user, mysql_api_pwd=mysql_api_pwd,
                         mysql_api_db=mysql_api_db, aws_access_key_id=aws_access_key_id,
                         aws_secret_access_key=aws_secret_access_key, s3_upload_bucket=s3_upload_bucket,
                         s3_upload_key_prefix=s3_upload_key_prefix, video_download_dir=video_download_dir,
                         console_log_level=console_log_level, log_file=log_file, file_log_level=file_log_level,
                         ffmpeg_preset_webp=ffmpeg_preset_webp, log_interval=log_interval,
                         log_backup_count=log_backup_count)

        self.logger.debug(f"[WebpWorker] rbmq_url={self.rbmq_url}, rbmq_alt_urls={self.rbmq_alt_urls}")
        # Setup download mode
        if s3_download_key_prefix is not None and s3_download_bucket is not None:
            self.download_mode = "s3"
        else:
            s3_download_bucket, s3_download_key_prefix = s3_upload_bucket, s3_upload_key_prefix
            self.download_mode = "s3"

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
        self.audio_bitrate = audio_bitrate
        self.s3_upload_song_key_prefix = s3_upload_song_key_prefix.strip("/")


    def get_consumers(self, Consumer, channel):
        return [
            Consumer(queues=self.inbound_queue,
                     callbacks=[self.on_message],
                     accept=['pickle', 'json', 'text/plain'],
                     prefetch_count=1)]

    def on_message(self, body, message):
        self.logger.debug(f"[WebpWorker][on_message] RECEIVED MESSAGE: {message}")
        try:
            recording_id = None
            push = False
            on_message_log = {
                "on_message_log": json.loads(message.payload),
                "queue_type": "webp_queue"
            }
            self.logger.info(json.dumps(on_message_log))
            file_log = {
                "webp_info": {
                    "type": "webp_conversion_log",
                    "worker_type": "webp_queue",
                },
                "time_start": str(datetime.datetime.utcnow()),
                "video_metadata": dict(),
                "message_from_queue": dict(),
                "procedures_mp4_to_webp": dict(),
                "procedures_mp4_to_mp3": dict()
            }
            if("recording_id" in json.loads(message.payload)):
                recording_id = str(json.loads(message.payload)["recording_id"])
                self.logger.debug(f"[WebpWorker][on_message] recording_id={recording_id}")
                file_log["message_from_queue"]["recording_id"] = recording_id
            else:
                self.logger.debug(f"[WebpWorker][on_message] recording_id key error")
                file_log["message_from_queue"]["recording_id"] = recording_id

            if("time" in json.loads(message.payload)):
                time = json.loads(message.payload)["time"]
                self.logger.debug(f"[WebpWorker][on_message] time={time}")
                file_log["message_from_queue"]["time"] = time
            else:
                self.logger.debug(f"[WebpWorker][on_message] time key error")
                # Msg doesn't provide timestamp info: set to -1
                file_log["message_from_queue"]["time"] = -1

            # There is a cron job trying to re-submit failed recording_id to  'video_upload_complete' queue
            # The queue msg will have 'push' key in payload
            if ("push" in json.loads(message.payload)):
                push = json.loads(message.payload)["push"]
                self.logger.debug(f"[WebpWorker][on_message] key exists: 'push'={push}'")

                file_log["message_from_queue"]["push"] = str(push)
            else:
                self.logger.debug(f"[WebpWorker][on_message] push key error")
                # Msg doesn't provide timestamp info: set to -1
                file_log["message_from_queue"]["push"] = 'None'


            # Dump log
            self.logger.info(json.dumps(file_log))
            if (recording_id is not None):
                self.process_recording(recording_id, file_log)
                # if (push):
                #     # write outbound message
                #     _, td = self._write_outbound_message(recording_id, push)

        except Exception as e:
            self.logger.debug(f"[WebpWorker][on_message] error={e}")

        finally:
            self.logger.debug(f"[WebpWorker][on_message] msg acked!!!!!!")
            message.ack()


    def process_recording(self, recording_id, file_log):
        run_path = None
        tic = time.time()

        try:
            # setup run_path
            run_path = self.video_download_path.joinpath(recording_id)
            run_path.mkdir(parents=True, exist_ok=True)
            self.logger.debug(f"[process_recording][{recording_id}] Run path: {run_path}")
            file_log["procedures_mp4_to_webp"]["run_path"] = str(run_path)

            # download video
            mp4_file = str(run_path.joinpath(f"{recording_id}.mp4"))
            self.logger.debug(f"[process_recording][{recording_id}] mp4_file={mp4_file}")
            try:
                # Get cover_start_time and url by recording_id
                cst = 1
                url = None
                song_id = None
                user_id = None
                total_time = 0
                fields = ['cover_start_time', 'url', 'song_id', 'user_id', 'total_time']
                values = DBoperation.get_values_from_db(table='recording', id=recording_id, fields=fields,
                                                        config=self.read_db_config, logger=self.logger)
                self.logger.debug(f"[process_recording][{recording_id}] values={values}")

                if (values is not None):

                    if values[0] is None:
                        cst = 1
                        self.logger.debug(f"[process_recording][{recording_id}] cst is None, set cover_start_time to {cst}")
                    else:
                        cst = values[0]
                        self.logger.debug(f"[process_recording][{recording_id}] cst={cst}")

                    if values[1] is None:
                        self.logger.debug(f"[process_recording][{recording_id}] url is None")
                        raise ValueError(f"[process_recording][{recording_id}] url is None")
                    else:
                        url = values[1]
                        self.logger.debug(f"[process_recording][{recording_id}] url={url}")

                    if values[2] is None:
                        self.logger.debug(f"[process_recording][{recording_id}] song_id is None")
                    else:
                        song_id = values[2]
                        self.logger.debug(f"[process_recording][{recording_id}] song_id={song_id}")

                    if values[3] is None:
                        self.logger.debug(f"[process_recording][{recording_id}] user_id is None")
                        raise ValueError(f"[process_recording][{recording_id}] user_id is None")
                    else:
                        user_id = values[3]
                        self.logger.debug(f"[process_recording][{recording_id}] user_id={user_id}")
                    if values[4] is None:
                        self.logger.debug(f"[process_recording][{recording_id}] total_time is None")
                    else:
                        total_time = values[4]
                        self.logger.debug(f"[process_recording][{recording_id}] total_time={total_time}")
                else:
                    raise ValueError(f"[process_recording][{recording_id}] values is None")

                self.logger.debug(f"[process_recording][{recording_id}] cst={cst}, url={url}, song_id={song_id}, "
                                  f"user_id={user_id}, total_time={total_time}")
                if (total_time - cst <= 1):
                    cst = total_time - 1
                if (cst < 0):
                    cst = 0

                #TODO: 'download_mode == http' is not yet supported
                if self.download_mode == "http":
                    _, td = self.download_video_from_url(local_file=mp4_file, url=url)
                elif self.download_mode == "s3":
                    self.logger.debug(f"[process_recording][{recording_id}] download_url={url}")
                    #TODO: Remove comment after testing
                    _, td = self.s3.s3_download_file(bucket=self.s3_download_bucket, key=url, local_file=mp4_file, logger=self.logger)
                else:
                    file_log["procedures_mp4_to_webp"]["download"] = {"success": False, "dl": 'download failed'}
                    raise ValueError(f"[process_recording][{recording_id}] download mode is not set properly: {self.download_mode}")
                file_log["procedures_mp4_to_webp"]["download"] = {"success": True, "td": td}
                file_log["video_metadata"]["original_size"] = os.path.getsize(mp4_file)
                self.logger.debug(f"[process_recording][{recording_id}] download_mode={self.download_mode}")
                self.logger.debug(f"[process_recording][{recording_id}] os.path.getsize({mp4_file})={os.path.getsize(mp4_file)}")

            except Exception as e:
                self.logger.debug(f"[process_recording][{recording_id}] download error={e}")
                file_log["procedures_mp4_to_webp"]["download"] = {"success": False, "exception": str(e)}
                self.logger.error(json.dumps(self.error_msg(recording_id, str(e), 'process_download')))
                raise e

            video_info = self.get_video_info(mp4_file, dimensions=True, fps=True)
            self.logger.debug(f"[process_recording][{recording_id}] Video info: {video_info}")
            width, height, fps = video_info["width"], video_info["height"], video_info["r_frame_rate"]
            # check vertical video or horizontal video
            vertical = True
            if width > height:
                vertical = False
                # -2 see also: https://stackoverflow.com/questions/20847674/ffmpeg-libx264-height-not-divisible-by-2
            file_log["procedures_mp4_to_webp"]["video_info"] = {"width": width, "height": height, "fps": fps,
                                                                "vertical": vertical, "cover_start_time": cst,
                                                                "total_time": total_time}

            # mp4_to_webp
            file_log, td = self.process_mp4_to_webp(recording_id=recording_id, mp4_file=mp4_file,
                                     cover_start_time=cst, file_log=file_log, width=width, height=height,
                                     static_vframes=self.static_vframes, dynamic_vframes=self.dynamic_vframes,
                                     lossless=self.lossless, jpg_w_size=self.jpg_w_size, jpg_h_size=self.jpg_h_size,
                                     webp_w_size=self.webp_w_size, webp_h_size=self.webp_h_size,
                                     webp_w_small_size=self.webp_w_small_size, webp_h_small_size=self.webp_h_small_size,
                                     webp_loop=self.webp_loop, webp_fps=self.webp_fps, vertical=vertical)
            self.logger.debug(f"[process_recording][{recording_id}] mp4 to webp is done, td={td}")
            file_log["procedures_mp4_to_webp"]["td"] = td

            file_log, td = self.process_mp4_to_mp3(recording_id=recording_id, mp4_file=mp4_file, song_id=song_id,
                                                   user_id=user_id, total_time=total_time, file_log=file_log)
            file_log["procedures_mp4_to_mp3"]["result"] = {"success": True, "td": td}
            self.logger.info(json.dumps(file_log))

        except Exception as e:
            # don't let the worker stopped by exceptions
            file_log["webp_info"]["complete"] = {"success": False, "exception": str(e)}
            self.logger.error(json.dumps(self.error_msg(recording_id, str(e), 'process_recording')))
        finally:
            #TODO Err msg to queue
            # write file log, success => info,  failed => error
            toc = time.time()
            file_log["time_stop"] = str(datetime.datetime.utcnow())
            file_log["time_elapsed"] = f"{toc - tic:.3f}"
            self.logger.debug(f"[process_recording][{recording_id}] finally: file_log:{file_log}")
            self.logger.info(json.dumps(file_log))

            # cleanup local files
            if run_path is not None:
                self.logger.debug(f"[process_recording][{recording_id}]  Clean_folder={self.clean_folder}, Cleanup {run_path}")
                if (self.clean_folder == 'True'):
                    self.logger.debug(f"[process_recording][{recording_id}] Ready to cleanup {run_path}")
                    shutil.rmtree(str(run_path))



    # Refer to https://bitbucket.org/LangLiveT/short-video-server/src/master/common/config/transcoder.php
    @timer
    def process_mp4_to_webp(self, recording_id, mp4_file, cover_start_time, file_log, width, height, static_vframes=1,
                            dynamic_vframes=9, lossless=1, jpg_w_size='1334', jpg_h_size='750', webp_w_size='667',
                            webp_h_size='375', webp_w_small_size='333', webp_h_small_size='187', webp_loop=0,
                            webp_fps=10, vertical=True):
        output_format = ['cover.jpg', 'cover_static.webp', 'cover_static_small.webp', 'cover_dynamic.webp',
                         'cover_dynamic_small.webp', 'first_cover_static.webp']
        """" 
        Example webp path:
        test/recording/288265560523801655/cover_static.webp
        test/recording/288265560523801655/first_cover_static.webp
        test/recording/288265560523801655/cover_static_small.webp
        test/recording/288265560523801655/cover_dynamic.webp
        test/recording/288265560523801655/cover_dynamic_small.webp
        test/recording/288265560523801655/cover.jpg
        """
        self.logger.debug(f"[process_mp4_to_webp][{recording_id}] mp4_file={mp4_file}, output_format={output_format}")
        # update width, height
        update_dict = {'width': width, 'height': height}
        for output in output_format:
            output_file = mp4_file.replace(f'{recording_id}.mp4', output)
            self.logger.debug(f"[process_mp4_to_webp][{recording_id}] output={output}, output_file={output_file}")
            cmd = None
            if (output == 'cover.jpg'):
                vf = f'scale=min(iw*{jpg_w_size}/ih\,{jpg_h_size}):min({jpg_w_size}\,ih*{jpg_h_size}/iw)'
                if (not vertical):
                    vf = f'scale=min(iw*{jpg_h_size}/ih\,{jpg_w_size}):min({jpg_h_size}\,ih*{jpg_w_size}/iw)'
                cmd = f'ffmpeg -y -i {mp4_file} -loglevel panic -ss {cover_start_time} -vf "{vf}" -preset {self.ffmpeg_preset_webp} -lossless {lossless} -an -vframes {static_vframes} {output_file}'

            elif (output == 'cover_static.webp'):
                vf = f'scale=min(iw*{webp_w_size}/ih\,{webp_h_size}):min({webp_w_size}\,ih*{webp_h_size}/iw)'
                if (not vertical):
                    vf = f'scale=min(iw*{webp_h_size}/ih\,{webp_w_size}):min({webp_h_size}\,ih*{webp_w_size}/iw)'
                cmd = f'ffmpeg -y -i {mp4_file} -loglevel panic -ss {cover_start_time} -vcodec libwebp -vf "{vf}" -preset {self.ffmpeg_preset_webp} -lossless {lossless} -an -vframes {static_vframes} {output_file}'

            elif (output == 'first_cover_static.webp'):
                vf = f'scale=min(iw*{webp_w_size}/ih\,{webp_h_size}):min({webp_w_size}\,ih*{webp_h_size}/iw)'
                if (not vertical):
                    vf = f'scale=min(iw*{webp_h_size}/ih\,{webp_w_size}):min({webp_h_size}\,ih*{webp_w_size}/iw)'
                cmd = f'ffmpeg -y -i {mp4_file} -loglevel panic -ss 0 -vcodec libwebp -vf "{vf}" -preset {self.ffmpeg_preset_webp} -lossless {lossless} -an -vframes {static_vframes} {output_file}'

            elif (output == 'cover_static_small.webp'):
                vf = f'scale=min(iw*{webp_w_small_size}/ih\,{webp_h_small_size}):min({webp_w_small_size}\,ih*{webp_h_small_size}/iw)'
                if (not vertical):
                    vf = f'scale=min(iw*{webp_h_small_size}/ih\,{webp_w_small_size}):min({webp_h_small_size}\,ih*{webp_w_small_size}/iw)'
                cmd = f'ffmpeg -y -i {mp4_file} -loglevel panic -ss {cover_start_time} -vcodec libwebp -vf "{vf}" -preset {self.ffmpeg_preset_webp} -lossless {lossless} -an -vframes {static_vframes} {output_file}'

            elif (output == 'cover_dynamic.webp'):
                vf = f'fps={webp_fps},scale=min(iw*{webp_w_size}/ih\,{webp_h_size}):min({webp_w_size}\,ih*{webp_h_size}/iw)'
                if (not vertical):
                    vf = f'fps={webp_fps},scale=min(iw*{webp_h_size}/ih\,{webp_w_size}):min({webp_h_size}\,ih*{webp_w_size}/iw)'
                cmd = f'ffmpeg -y -i {mp4_file} -loglevel panic -ss {cover_start_time} -vframes {dynamic_vframes} -vcodec libwebp -vf "{vf}" -preset {self.ffmpeg_preset_webp} -lossless {lossless} -an -loop {webp_loop} {output_file}'

            elif (output == 'cover_dynamic_small.webp'):
                vf = f'fps={webp_fps},scale=min(iw*{webp_w_small_size}/ih\,{webp_h_small_size}):min({webp_w_small_size}\,ih*{webp_h_small_size}/iw)'
                # vf = f'fps={webp_fps},scale=270:480'
                if (not vertical):
                    # vf = f'fps={webp_fps},scale=min(iw*{webp_h_small_size}/ih\,{webp_w_small_size}):min({webp_h_small_size}\,ih*{webp_w_small_size}/iw)'
                    # vf = f'fps={webp_fps},scale=480:270' # about 200KB
                    # vf = f'crop=1/2*in_w:in_h:5/16*in_w:0,fps={webp_fps},scale=240:270'
                    vf = f'crop=1/2*in_w:in_h:1/4*in_w:0,fps={webp_fps},scale=240:270' # about 40-60KB
                cmd = f'ffmpeg -y -i {mp4_file} -loglevel panic -ss {cover_start_time} -vframes {dynamic_vframes} -vcodec libwebp -vf "{vf}" -preset {self.ffmpeg_preset_webp} -lossless {lossless} -an -loop {webp_loop} {output_file}'

            else:
                raise ValueError(f"[WebpBase][process_mp4_to_webp] output = {output} is not correct")

            self.logger.debug(f"[WebpBase][process_mp4_to_webp] Process {mp4_file} with command= {cmd}")
            res = subprocess.call(cmd, shell=True)
            self.logger.debug(f"[WebpBase][process_mp4_to_webp] res = {res}")
            file_log["procedures_mp4_to_webp"]["cmd_result"] = {"cmd": cmd, "result": res}

            # check upload file size
            upload_size = os.path.getsize(output_file)
            self.logger.debug(f"[WebpBase][process_mp4_to_webp][{recording_id}] upload_size={upload_size}")
            if upload_size == 0:
                e = ValueError("file size should be greater than 0")
                file_log["procedures_mp4_to_webp"]["sizecheck"] = {"success": False, "exception": str(e)}
                self.logger.error(json.dumps(self.error_msg(recording_id, str(e), 'procedures_mp4_to_webp')))
                raise e
            else:
                file_log["procedures_mp4_to_webp"]["sizecheck"] = {"success": True, "bytes": upload_size}
            #TODO JSON LOG, EVENT, RECORDING_ID AS STR "123455678"
            self.logger.debug(f"[WebpBase][process_mp4_to_webp][{recording_id}] output_file: {output_file}")

            if output_file is not None:
                # upload video
                try:
                    upload_key = f"{self.s3_upload_key_prefix}/{recording_id}/{output}"

                    if(output == 'cover.jpg'):
                        contentType='image/jpeg'
                    else:
                        contentType='image/webp'
                    self.logger.debug(f"[WebpBase][process_mp4_to_webp][{recording_id}] upload_key={upload_key}")

                    #TODO: S3 test comment out
                    _, td= self.s3.s3_upload_file(bucket=self.s3_upload_bucket,
                                                key=upload_key,
                                                upload_file=output_file,
                                                logger=self.logger,
                                                retries=10,
                                                content_type=contentType)

                    file_log["procedures_mp4_to_webp"]["upload"] = {"success": True, "td": td}
                except Exception as e:
                    file_log["procedures_mp4_to_webp"]["upload"] = {"success": False, "exception": str(e)}
                    self.logger.error(json.dumps(self.error_msg(recording_id, str(e), 'procedures_mp4_to_webp_upload')))
                    raise e

                field = self.field_mapping(output)
                update_dict[field] = upload_key
                self.logger.debug(f"[WebpBase][process_mp4_to_webp][{recording_id}] add field={field}, value={upload_key} to update_dict")

            else:
                self.logger.debug(f"[WebpBase][process_mp4_to_webp][{recording_id}] output_file={output_file}")
                file_log["procedures_mp4_to_webp"]["output_file"] = {"success": False, "output_file": output_file}
        #TODO: move try catch
        try:
            _ = DBoperation.update_db(table='recording', update_map=update_dict, id=recording_id,
                                      config=self.write_db_config, logger=self.logger)
            file_log["procedures_mp4_to_webp"]["update_recording"] = {"success": True, "update_field_len": len(update_dict)}
            file_log["webp_info"]["complete"] = {"success": True}
        except Exception as e:
            file_log["procedures_mp4_to_webp"]["update_recording"] = {"success": False, "exception": e}
            self.logger.error(json.dumps(self.error_msg(recording_id, str(e), 'procedures_mp4_to_webp_update_db')))
            raise e

        finally:
            self.logger.debug(f"[WebpBase][process_mp4_to_webp][{recording_id}] Done")
            return file_log

    def field_mapping(self, file_type):
        res = None
        if(file_type == 'cover.jpg'):
            res = 'jpg_cover_url'
        elif (file_type == 'cover_static.webp'):
            res = 'static_cover_url'
        elif (file_type == 'first_cover_static.webp'):
            res = 'first_static_cover_url'
        elif (file_type == 'cover_static_small.webp'):
            res = 'small_static_cover_url'
        elif (file_type == 'cover_dynamic.webp'):
            res = 'dynamic_cover_url'
        elif (file_type == 'cover_dynamic_small.webp'):
            res = 'small_dynamic_cover_url'
        else:
            raise ValueError(f"[WebpBase][field_mapping] not matched")
        return res

    # """ 'push = True' means 'audit pass'"""
    # @timer
    # def _write_outbound_message(self, recording_id, push=False):
    #     self.logger.debug(f"[write_outbound_message] Write outbound message for {recording_id}, push={push}")
    #     outbound_message = {
    #         "recording_id": recording_id,
    #         "time": time.time(),
    #         "push": push
    #     }
    #
    #     retry_policy = {
    #         "interval_start": 0,
    #         "interval_step": 5,
    #         "interval_max": 30,
    #         "max_retries": 20,
    #     }
    #     self.producer.publish(body=json.dumps(outbound_message),
    #                           routing_key=self.outbound_queue.routing_key,
    #                           exchange=self.exchange,
    #                           # declare exchange, queue and bind
    #                           declare=[self.outbound_queue],
    #                           serializer="json",
    #                           retry=True,
    #                           retry_policy=retry_policy)

    # Refer to https://bitbucket.org/LangLiveT/short-video-server/src/master/common/config/transcoder.php
    """
    ffmpeg -y -i /xxx/xxx/{recording_id}-master.mp4 -f mp3 -ab 192000 -vn /xxx/xxx/{recording_id}-song.mp3
    -vn: Disable video, to make sure no video (including album cover image) is included if the source would be a video
    -f: force-output
    """
    @timer
    def process_mp4_to_mp3(self, recording_id, mp4_file, song_id, user_id, total_time, file_log):
        audio_file = mp4_file.replace(f'.mp4', '-song.mp3')
        cmd = f'ffmpeg -y -i {mp4_file} -loglevel panic -f mp3 -ab 19200 -vn {audio_file}'
        file_log["procedures_mp4_to_mp3"]["recording_song_id"] = song_id
        file_log["procedures_mp4_to_mp3"]["audio_file"] = audio_file
        result = False
        # iOS song_id = 0 means song type is not selected.
        if (song_id is None or song_id is 0):
            self.logger.debug(f"[process_mp4_to_mp3] Process {mp4_file} with command= {cmd}")
            res = subprocess.call(cmd, shell=True)
            file_log["procedures_mp4_to_mp3"]["cmd_result"] = {"cmd": cmd, "result": res}

            self.logger.debug(f"[process_mp4_to_mp3] res = {res}")
            if (res == 0):
                result = True
            else:
                self.logger.error(json.dumps(self.error_msg(recording_id, f'make mp3 failed, res={res}', 'process_mp4_to_mp3')))

        if (result and os.path.exists(audio_file)):
            # createSongByRecording
            self.logger.debug(f"[process_mp4_to_mp3] result = {result}, audio_file={audio_file}")
            self.logger.debug(f"[process_mp4_to_mp3] os.path.getsize({audio_file}) = {os.path.getsize(audio_file)}")

            if (os.path.getsize(audio_file) is not 0):
                # Create and update song object
                file_log, td = self.createSongByRecording(recording_id=recording_id, user_id=user_id, total_time=total_time, audio_file=audio_file, file_log=file_log)
                file_log["procedures_mp4_to_mp3"]["createSongByRecording"] = {"success": True, "td": td}

        return file_log

    """
    Refer to : https://bitbucket.org/LangLiveT/short-video-server/src/a0adddb4f891e14c988c5fb2c4b7213ff06cb7c9/common/services/SongService.php#lines-396
    """
    @timer
    def createSongByRecording(self, recording_id, user_id, total_time, audio_file, file_log):
        try:
            # Get author info
            fields = ['nick_name', 'avatar']
            self.logger.debug(f"[createSongByRecording] user_id={user_id}, total_time={total_time}, audio_file={audio_file}")

            userInfo = DBoperation.get_values_from_db(table='user', id=user_id, config=self.read_db_config,
                                                      fields=fields, id_key='user_id', logger=self.logger)
            self.logger.debug(f"[createSongByRecording] usrInfo={userInfo}")

            nick_name = ''
            avatar = ''
            if userInfo[0] is None:
                self.logger.debug(f"[createSongByRecording] nick_name is None, set nick_name to empty str")
            else:
                nick_name = userInfo[0]
                self.logger.debug(f"[createSongByRecording] nick_name={nick_name}")

            if userInfo[1] is None:
                self.logger.debug(f"[createSongByRecording] avatar is None")
            else:
                avatar = userInfo[1]
                self.logger.debug(f"[createSongByRecording] avatar={avatar}")

            name = f"""@{nick_name}創作的原聲"""
            cover = avatar
            duration = total_time
            create_time = time.time()
            url = ''
            deleted = 0
            status = 2
            tag_id = 0
            tag_name = ''
            insert_data = [name, cover, duration, url, deleted, status, tag_id, tag_name, create_time]
            song_id = DBoperation.insert_song_db(data=insert_data, config=self.write_db_config, logger=self.logger)
            file_log["procedures_mp4_to_mp3"]["insert_db"] = {"success": True, 'song_id': song_id}

            # test/song/2570/song.mp3
            upload_key = f"{self.s3_upload_song_key_prefix}/{song_id}/song.mp3"
            self.logger.debug(f"[createSongByRecording] user_id={user_id}, upload_key={upload_key}")

            # TODO: S3 test comment out
            _, td = self.s3.s3_upload_file(bucket=self.s3_upload_bucket,
                                           key=upload_key,
                                           upload_file=audio_file,
                                           logger=self.logger,
                                           retries=10,
                                           content_type='audio/mpeg')
            file_log["procedures_mp4_to_mp3"]["upload_mp3"] = {"success": True, "td": td}

            update_map = {'url': upload_key}
            _ = DBoperation.update_db(table='song', id=song_id, update_map=update_map, config=self.write_db_config, logger=self.logger)
            file_log["procedures_mp4_to_mp3"]["update_url"] = {"success": True}

            update_map = {'song_id': song_id}
            _ = DBoperation.update_db(table='recording', id=recording_id, update_map=update_map, config=self.write_db_config, logger=self.logger)
            file_log["procedures_mp4_to_mp3"]["update_song_id"] = {"success": True}

        except Exception as e:
            file_log["procedures_mp4_to_mp3"]["createSongByRecording"] = {"success": False, "exception": str(e)}
            self.logger.error(json.dumps(self.error_msg(recording_id, str(e), 'createSongByRecording')))
            raise e
        finally:
            self.logger.debug(f"[createSongByRecording] Finally")
            return file_log