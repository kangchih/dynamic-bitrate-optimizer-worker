import os
from workers.base import VmafBase
import datetime
import time
import json
import shutil
from module import DBoperation
from utils import send_err_email, timer
import hashlib
import subprocess
from apscheduler.schedulers.background import BackgroundScheduler



class WatermarkWorker(VmafBase):

    def __init__(self, mysql_api_host_write, mysql_api_host_read, mysql_api_port, mysql_api_user, mysql_api_pwd,
                 mysql_api_db, mysql_activity_host_write, mysql_activity_host_read, mysql_activity_port,
                 mysql_activity_user, mysql_activity_pwd, mysql_activity_db, tag, aws_access_key_id,
                 aws_secret_access_key,
                 s3_upload_bucket, s3_upload_key_prefix, logo_file,
                 video_download_dir, console_log_level="DEBUG",
                 file_log_level="INFO", log_file=None, s3_download_bucket=None, s3_download_key_prefix=None,
                 max_video_height=960, max_video_width=960, max_fps=30, crf_start=26, crf_stop=33, crf_step=2,
                 ffmpeg_preset='slow', ffmpeg_preset_webp='default', log_interval=5, log_backup_count=20,
                 clean_folder='True', smart_download='False', task_interval_sec=600):


        self.clean_folder = clean_folder
        self.smart_download = smart_download
        self.task_interval_sec = task_interval_sec

        self.write_activity_db_config = {'host': mysql_activity_host_write, 'port': mysql_activity_port,
                                         'user': mysql_activity_user,
                                         'password': mysql_activity_pwd,
                                         'database': mysql_activity_db, 'charset': "utf8mb4"}

        self.read_activity_db_config = {'host': mysql_activity_host_read, 'port': mysql_activity_port,
                                        'user': mysql_activity_user,
                                        'password': mysql_activity_pwd,
                                        'database': mysql_activity_db, 'charset': "utf8mb4"}

        self.tag = tag
        self.logo_file = logo_file
        super().__init__(mysql_api_host_write=mysql_api_host_write, mysql_api_host_read=mysql_api_host_read,
                         mysql_api_port=mysql_api_port, mysql_api_user=mysql_api_user, mysql_api_pwd=mysql_api_pwd,
                         mysql_api_db=mysql_api_db, aws_access_key_id=aws_access_key_id,
                         aws_secret_access_key=aws_secret_access_key, s3_upload_bucket=s3_upload_bucket,
                         s3_upload_key_prefix=s3_upload_key_prefix, video_download_dir=video_download_dir,
                         console_log_level=console_log_level, log_file=log_file,
                         file_log_level=file_log_level, max_video_height=max_video_height,
                         max_video_width=max_video_width, max_fps=max_fps, crf_start=crf_start, crf_stop=crf_stop,
                         crf_step=crf_step, ffmpeg_preset=ffmpeg_preset, ffmpeg_preset_webp=ffmpeg_preset_webp,
                         log_interval=log_interval, log_backup_count=log_backup_count)
        # Setup download mode
        #TODO: Refactor http download mode
        if s3_download_key_prefix is not None and s3_download_bucket is not None:
            self.download_mode = "s3"
        else:
            s3_download_bucket, s3_download_key_prefix = s3_upload_bucket, s3_upload_key_prefix
            self.download_mode = "s3"

        # self.video_upload_name = hashlib.md5(str(time.time()).encode('utf-8')).hexdigest() +'-wm.mp4'
        self.s3_download_bucket = s3_download_bucket


    def run(self):
        self.logger.debug(f"====================== Watermark run ======================")
        scheduler = BackgroundScheduler()
        self.logger.debug(f"[watermark][run] scheduler interval: {self.task_interval_sec}")
        scheduler.add_job(self.getNRunWatermark, 'interval', seconds=self.task_interval_sec)
        scheduler.start()
        try:
            # This is here to simulate application activity (which keeps the main thread alive).
            while True:
                time.sleep(2)
        except Exception as e:
            self.logger.debug(f"[watermark][run] Exception caught!! scheduler shut down!")
            self.logger.error(json.dumps(self.error_msg("watermark error", str(e), 'process_watermark', "watermark")))
            # Not strictly necessary if daemonic mode is enabled but should be done if possible
            scheduler.shutdown()
            # don't let the worker stopped by exceptions
        finally:
            #TODO Err msg to queue
            # write file log, success => info,  failed => error
            self.logger.debug(f"[process_watermark][run] finally")


    def process_watermark(self, recording_id):
        file_log = {
            "watermark_info": {
                "type": "watermark_conversion_log",
                "worker_type": "watermark",
                "watermark_tag": self.tag,
            },
            "time_start": str(datetime.datetime.utcnow()),
            "wm_process": dict()
        }
        run_path = None
        tic = time.time()
        success = False
        url = None
        try:
            # setup run_path
            run_path = self.video_download_path.joinpath(recording_id)
            run_path.mkdir(parents=True, exist_ok=True)
            self.logger.debug(f"[process_watermark][{recording_id}] Run path: {run_path}")
            file_log["wm_process"]["run_path"] = str(run_path)

            # download video
            mp4_file = str(run_path.joinpath(f"{recording_id}.mp4"))
            self.logger.debug(f"[process_watermark][{recording_id}] mp4_file={mp4_file}")
            file_log["wm_process"]["run_path"] = str(run_path)
            file_log["wm_process"]["mp4_file"] = {"mp4_file": mp4_file, "exist": os.path.exists(mp4_file)}

            try:
                # If mp4_file exists, no need to download again
                # NOTE: If smart_download = False => s3 download will be triggered anyway
                if((not os.path.exists(mp4_file)) or (self.smart_download is 'False')):
                    # Get url, dbo_url by recording_id
                    q_fields = ['url', 'dbo_url', 'watermark_video_url']
                    rInfo = DBoperation.get_values_from_db(table='jungle2_watermark', id=recording_id, fields=q_fields,
                                                           config=self.read_activity_db_config, id_key='recording_id',
                                                           logger=self.logger)
                    self.logger.debug(f"[process_watermark][{recording_id}] rInfo={rInfo}")
                    if (len(rInfo) == len(q_fields)):
                        url = rInfo[0]
                        dbo_url = rInfo[1]
                        wm_url = rInfo[2]
                        self.logger.debug(f"[process_watermark][{recording_id}] url={url}, dbo_url={dbo_url},"
                                          f" wm_url={wm_url}")

                        if(wm_url is not None and wm_url != ''):
                            self.logger.debug(f"[process_watermark][{recording_id}] watermark has been done")
                            return True

                        if (dbo_url is not None and dbo_url != ''):
                            url = dbo_url
                            self.logger.debug(f"[process_watermark][{recording_id}] dbo_url exists! url=dbo_url={url}")

                    else:
                        self.logger.debug(f"[Error][process_watermark][{recording_id}] rInfo={rInfo}")
                        raise ValueError(f"[Error][process_watermark][{recording_id}] rInfo={rInfo}")


                    if url is None:
                        raise ValueError(f"[process_watermark][{recording_id}] url is None")
                    file_log["wm_process"]["download_url"] = url
                    #TODO: 'download_mode == http' is not yet supported
                    if self.download_mode == "http":
                        _, td = self.download_video_from_url(local_file=mp4_file, url=url)
                    elif self.download_mode == "s3":
                        self.logger.debug(f"[process_watermark][{recording_id}] download_url={url}")
                        #TODO: Remove comment after testing
                        _, td = self.s3.s3_download_file(bucket=self.s3_download_bucket, key=url, local_file=mp4_file, logger=self.logger)
                    else:
                        file_log["wm_process"]["download"] = {"success": False, "dl": 'download failed'}
                        raise ValueError(f"[process_watermark][{recording_id}] download mode is not set properly: {self.download_mode}")
                    file_log["wm_process"]["download"] = {"success": True, "td": td}
                    self.logger.debug(f"[process_watermark][{recording_id}]download_mode={self.download_mode}, td={td}")
                else:
                    file_log["wm_process"]["download_mode"] = self.download_mode

            except Exception as e:
                self.logger.debug(f"[process_watermark][{recording_id}] download error={e}")
                file_log["wm_process"]["download"] = {"success": False, "exception": str(e)}
                self.logger.error(json.dumps(self.error_msg(recording_id, str(e), 'process_watermark', "watermark")))
                raise e

            # TODO: Check log
            self.logger.info(json.dumps(file_log))

            # Process mp4 to jpgs for face detect
            (upload_mp4_file, file_log), td = self.mp4_to_watermark(recording_id=recording_id, mp4_file=mp4_file,
                                                        logo_file=self.logo_file, run_path=run_path, file_log=file_log)
            self.logger.debug(f"[process_watermark][{recording_id}] upload_mp4_file={upload_mp4_file}, td={td}")

            file_log["wm_process"]["mp4_to_watermark"] = {"td": td}

            if (upload_mp4_file):
                # Check upload file size and upload video
                try:
                    upload_size = os.path.getsize(upload_mp4_file)
                    self.logger.debug(f"[process_watermark][{recording_id}] upload_size={upload_size}")
                    if upload_size == 0:
                        e = ValueError("file size should be greater than 0")
                        file_log["wm_process"]["sizecheck"] = {"success": False, "exception": str(e)}
                        self.logger.error(json.dumps(self.error_msg(recording_id, str(e), 'process_before_upload',
                                                                    "watermark")))
                        raise e
                    else:
                        file_log["wm_process"]["sizecheck"] = {"success": True, "bytes": upload_size}

                    video_upload_s3_name = hashlib.md5(str(time.time()).encode('utf-8')).hexdigest() + '-wm.mp4'
                    upload_key = f"{self.s3_upload_key_prefix}/{recording_id}/{self.tag}/{video_upload_s3_name}"
                    self.logger.debug(f"[process_watermark][{recording_id}] upload_key={upload_key}")
                    #TODO: S3 test comment out
                    _, td = self.s3.s3_upload_file(bucket=self.s3_upload_bucket,
                                                   key=upload_key,
                                                   upload_file=upload_mp4_file,
                                                   logger=self.logger)
                    file_log["wm_process"]["upload"] = {"success": True, "td": td, "upload_key": upload_key}
                    #TODO exception handler
                except Exception as e:
                    file_log["wm_process"]["upload"] = {"success": False, "exception": str(e)}
                    self.logger.error(json.dumps(self.error_msg(recording_id, str(e), 'process_upload', "watermark")))
                    raise e

                # update db with new meta
                try:
                    update_dict = {'watermark_video_url': upload_key}
                    _ = DBoperation.update_db(table='jungle2_watermark', update_map=update_dict, id=recording_id,
                                              config=self.write_activity_db_config, id_key='recording_id',
                                              logger=self.logger)
                    file_log["wm_process"]["update_recording"] = {"success": True}
                except Exception as e:
                    file_log["wm_process"]["update_recording"] = {"success": False, "exception": str(e)}
                    self.logger.error(json.dumps(self.error_msg(recording_id, str(e), 'process_update_db', "watermark")))
                    raise e
            else:
                file_log["wm_process"]["upload"] = {"ignore": True}
                file_log["wm_process"]["update_recording"] = {"ignore": True}
            file_log["watermark_info"]["complete"] = {"success": True}
            success = True

        except Exception as e:
            # don't let the worker stopped by exceptions
            file_log["watermark_info"]["complete"] = {"success": False, "exception": str(e)}
            self.logger.error(json.dumps(self.error_msg(recording_id, str(e), 'process_watermark', "watermark")))
        finally:
            #TODO Err msg to queue
            # write file log, success => info,  failed => error
            toc = time.time()
            file_log["time_stop"] = str(datetime.datetime.utcnow())
            file_log["time_elapsed"] = f"{toc - tic:.3f}"
            self.logger.debug(f"[process_watermark][{recording_id}] finally file_log:{file_log}")
            self.logger.debug(f"[process_watermark][{recording_id}] success:{success}")
            self.logger.info(json.dumps(file_log))

            # cleanup local files
            if run_path is not None:
                self.logger.debug(f"[process_watermark][{recording_id}] Clean_folder={self.clean_folder},"
                                  f" Cleanup {run_path}")
                if (self.clean_folder == 'True'):
                    self.logger.debug(f"[process_watermark][{recording_id}] Ready to cleanup {run_path}")
                    shutil.rmtree(str(run_path))
            return success


    @timer
    def mp4_to_watermark(self, recording_id, mp4_file, logo_file, run_path, file_log):
        """"
        Example path:
        video_cache/123456789012345678/xxx.mp4
        """
        watermark_path = f'{run_path}/watermark.mp4'
        self.logger.debug(f"[mp4_to_watermark][{recording_id}] mp4_file={mp4_file}, run_path={run_path}")
        # output_file = mp4_file.replace(f'{recording_id}.mp4', output)
        # cmd = f'ffmpeg -y -i {mp4_file} -loglevel panic -vf fps=1 {run_path}/detectface%03d.jpg'
        cmd = f'ffmpeg -y -loglevel panic -i {mp4_file} -i {logo_file} -filter_complex overlay=main_w-overlay_w-10:main_h-overlay_h-150 {run_path}/watermark.mp4'
        self.logger.debug(f"[mp4_to_watermark] Process {mp4_file} with command= {cmd}")
        res = subprocess.call(cmd, shell=True)
        file_log["wm_process"]["mp4_to_watermark"] = {"cmd": cmd, "result": res}
        self.logger.debug(f"[mp4_to_watermark] res = {res}")

        return watermark_path, file_log

    @timer
    def getNRunWatermark(self):
        todoList = DBoperation.get_todo_watermark_rid(config=self.read_activity_db_config, table='jungle2_watermark',
                                                      logger=self.logger)
        # self.logger.debug(f"[getNRunWatermark] todoList={todoList}")
        todo_log = {
            "watermark_info": {
                "type": "watermark_conversion_log",
                "worker_type": "watermark",
                "watermark_tag": self.tag,
            },
            "time_start": str(datetime.datetime.utcnow()),
            "todo_list": str(list(todoList))
        }
        self.logger.info(json.dumps(todo_log))
        try:
            for rid, url, dbo_url in todoList:
                self.logger.debug(f"[getNRunWatermark] rid={rid}, url={url}, dbo_url={dbo_url}")
                success = self.process_watermark(str(rid))
                if (not success):
                    self.logger.debug(f"[getNRunWatermark] send alert email!!")
                    send_err_email("watermark error", worker_type='water_mark')
        except Exception as e:
            self.logger.debug(f"[getNRunWatermark] error={e}")
            self.logger.error(json.dumps(self.error_msg("watermark worker", str(e), 'run', "watermark")))
        finally:
            self.logger.debug(f"[WatermarkWorker] Finally")
