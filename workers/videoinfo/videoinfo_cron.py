from workers.base import VmafBase
import datetime
import time
import json
from module import DBoperation
from utils import send_err_email, timer
from apscheduler.schedulers.background import BackgroundScheduler



class VideoInfoWorker(VmafBase):

    def __init__(self, mysql_api_host_write, mysql_api_host_read, mysql_api_port, mysql_api_user, mysql_api_pwd,
                 mysql_api_db, aws_access_key_id="aws_access_key_id", aws_secret_access_key="aws_secret_access_key",
                 s3_upload_bucket='None', s3_upload_key_prefix='None', video_download_dir='None',
                 console_log_level="DEBUG", file_log_level="INFO", log_file=None,
                 path_prefix="https://langlive-video.s3-ap-southeast-1.amazonaws.com",
                 max_video_height=960, max_video_width=960, max_fps=30, crf_start=26, crf_stop=33, crf_step=2,
                 ffmpeg_preset='slow', ffmpeg_preset_webp='default', log_interval=5, log_backup_count=20,
                 clean_folder='True', task_interval_sec=600, task_limit=300):


        self.clean_folder = clean_folder
        self.task_interval_sec = task_interval_sec
        self.path_prefix = path_prefix
        self.task_limit = task_limit
        self.failed_rids = []

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



    def run(self):
        self.logger.debug(f"====================== Video Info run ======================")
        scheduler = BackgroundScheduler()
        self.logger.debug(f"[VideoInfo][run] scheduler interval: {self.task_interval_sec}")

        scheduler.add_job(self.getNRunVideoInfo, 'interval', seconds=self.task_interval_sec)
        scheduler.start()
        try:
            # This is here to simulate application activity (which keeps the main thread alive).
            while True:
                time.sleep(2)
        except Exception as e:
            self.logger.debug(f"[VideoInfo][run] Exception caught!! scheduler shut down!")
            self.logger.error(json.dumps(self.error_msg("video_info error", str(e), 'process_video_info', "video_info")))
            # Not strictly necessary if daemonic mode is enabled but should be done if possible
            scheduler.shutdown()
            # don't let the worker stopped by exceptions
        finally:
            #TODO Err msg to queue
            # write file log, success => info,  failed => error
            self.logger.debug(f"[process_video_info][run] finally")

    @timer
    def process_video_info_batch(self, data):

        tic = time.time()
        success = False
        update_ok_list = []
        url = None
        insertBulkData = []
        try:
            rids = [i[0] for i in data]
            self.logger.debug(f"[process_video_info] tic={tic}, len_rids={len(rids)}")

            # Get url, dbo_url by recording_id
            q_fields = ['id', 'url', 'dbo_url', 'width', 'height']
            rInfo = DBoperation.get_values_from_db_batch(table='recording', ids=rids, fields=q_fields,
                                                         config=self.read_db_config, id_key='id',
                                                         logger=self.logger)
            for rid, url, dbo_url, width, height in rInfo:

                try:
                    file_log = {
                        "video_info": {
                            "type": "video_info_conversion_log",
                            "worker_type": "video_info"
                        },
                        "time_start": str(datetime.datetime.utcnow()),
                        "vi_process": dict()
                    }

                    self.logger.debug(f"[process_video_info][{rid}] url={url}, dbo_url={dbo_url}, height={height}, width={width}")

                    if (dbo_url is not None and dbo_url != ''):
                        url = dbo_url
                        self.logger.debug(f"[process_video_info][{rid}] dbo_url exists! url=dbo_url={url}")
                    else:
                        self.logger.debug(f"[process_video_info][{rid}] no dbo_url={dbo_url}")

                    if url is None:
                        raise ValueError(f"[process_video_info][{rid}] url is None")
                    file_log["vi_process"]["url"] = url


                    # TODO: Check log
                    self.logger.info(json.dumps(file_log))
                    # get video info
                    # (width, height, file_log), td = self.getVideoInfo(recording_id=rid, url=url, file_log=file_log)
                    recording_url = f"{self.path_prefix}/{url}"
                    video_info = self.get_video_info_cron(recording_url, dimensions=True)
                    self.logger.debug(f"[process_video_info][{rid}] recording_url={recording_url}, video info: {video_info}")
                    if video_info:
                        width_1, height_1 = video_info["width"], video_info["height"]
                        self.logger.debug(f"[process_video_info][{rid}] width={width_1}, height={height_1}")
                        file_log["vi_process"]["get_video_info"] = {"width": width_1, "height": height_1}
                        # success = True
                        if (width_1 != '' and height_1 != '' and width_1 != 0 and height_1 != 0):
                            insertData = (width_1, height_1, rid)
                            self.logger.debug(f"[process_video_info][{rid}] insertData={insertData}")

                            insertBulkData.append(insertData)
                            update_ok_list.append(rid)

                        else:
                            self.logger.debug(f"[WARN][process_video_info][{rid}] width={width}, height={height}")
                    else:
                        if rid in self.failed_rids:
                            print(f"[ERROR][process_video_info][{rid}] rid already in failed_rids.")
                        else:
                            print(f"[ERROR][process_video_info][{rid}] get video info error. Append to failed_rids.")
                            self.failed_rids.append(rid)
                            print(f"[ERROR][process_video_info][{rid}] len_failed_rids={len(self.failed_rids)}")

                except Exception as e:
                    if rid in self.failed_rids:
                        print(f"[ERROR][process_video_info][{rid}] rid already in failed_rids.")
                    else:
                        print(f"[ERROR][process_video_info][{rid}] get video info error. Append to failed_rids.")
                        self.failed_rids.append(rid)
                        print(f"[ERROR][process_video_info][{rid}] len_failed_rids={len(self.failed_rids)}")
                    file_log["vi_process"]["get_video_info_error"] = {"success": False, "exception": str(e), "len_failed_rids": f"{len(self.failed_rids)}"}
                    self.logger.error(
                        json.dumps(self.error_msg(rid, str(e), 'get_video_info_error', "videoinfo")))
                    # raise e
            # update db with new meta
            try:
                _ = DBoperation.bulk_update_height_width_db(config=self.write_db_config, data=insertBulkData,
                                                            logger=self.logger)
                file_log["vi_process"]["complete"] = {"success": True}
                success = True
            except Exception as e:
                file_log["vi_process"]["update_recording"] = {"success": False, "exception": str(e)}
                self.logger.error(json.dumps(self.error_msg(rid, str(e), 'bulk_update_height_width_db', "videoinfo")))
                raise e

        except Exception as e:
            # don't let the worker stopped by exceptions
            file_log["video_info"]["complete"] = {"success": False, "exception": str(e)}
            self.logger.error(json.dumps(self.error_msg(rid, str(e), 'process_video_info', "video_info")))
        finally:
            #TODO Err msg to queue
            # write file log, success => info,  failed => error
            toc = time.time()
            file_log["time_stop"] = str(datetime.datetime.utcnow())
            file_log["time_elapsed"] = f"{toc - tic:.3f}"
            file_log["update_ok_list"] = f"{str(update_ok_list)}"
            file_log["len_update_ok_list"] = f"{len(update_ok_list)}"
            file_log["failed_rids_size"] = f"{len(self.failed_rids)}"
            self.logger.debug(f"[process_video_info][{rid}] finally file_log:{file_log}")
            self.logger.debug(f"[process_video_info][{rid}] success:{success}")
            self.logger.info(json.dumps(file_log))
            return update_ok_list

    # @timer
    # def getVideoInfo(self, recording_id, url, file_log):
    #     self.logger.debug(f"[getVideoInfo][{recording_id}] url={url}")
    #
    #     # cmd = f'ffprobe -v error -select_streams v:0 -show_entries stream=width,height -of csv=s=x:p=0 {self.path_prefix}/{url}'
    #     cmd = f'ffprobe -v error -select_streams v:0 -show_entries stream=width,height -of csv=s=x:p=0  https://static.imvideo.app/recording/288265560524212517/master.mp4'
    #     self.logger.debug(f"[getVideoInfo][{recording_id}] Process {url} with command= {cmd}")
    #     res = subprocess.call(cmd, shell=True)
    #     file_log["vi_process"]["getVideoInfo"] = {"cmd": cmd, "result": res}
    #     self.logger.debug(f"[getVideoInfo] res = {res}")
    #     width = 0
    #     height = 0
    #     if 'x' in res:
    #         width = res.split('x')[0]
    #         height = res.split('x')[1]
    #
    #     return int(width), int(height), file_log


    @timer
    def getNRunVideoInfo(self):
        todoList = DBoperation.get_todo_video_info_rid(config=self.read_db_config, failed_rids=self.failed_rids,
                                                       table='recording', logger=self.logger, limit=self.task_limit)
        todo_ids = [i[0] for i in todoList]
        todo_log = {
            "video_info": {
                "type": "video_info_conversion_log",
                "worker_type": "video_info"
            },
            "time_start": str(datetime.datetime.utcnow()),
            "len_todo": len(todoList),
            "todo": str(todo_ids)
        }
        self.logger.info(json.dumps(todo_log))
        try:

            update_ok_list, td = self.process_video_info_batch(todoList)
            self.logger.debug(f"[getNRunVideoInfo] update_ok_list={update_ok_list}, td={td}")

            # if (not success):
            #     self.logger.debug(f"[getNRunVideoInfo] send alert email!!")
            #     send_err_email(str(datetime.datetime.utcnow()), worker_type='video_info')

        except Exception as e:
            self.logger.debug(f"[getNRunVideoInfo] error={e}")
            self.logger.error(json.dumps(self.error_msg("video_info worker", str(e), 'run', "video_info")))
        finally:
            self.logger.debug(f"[getNRunVideoInfo] Finally")
