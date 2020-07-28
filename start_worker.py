from config import load_config
from workers import use_worker
import logging
import os
from pathlib import Path
import posixpath
import datetime
import json


if __name__ == '__main__':
    build_version = '1.0.6'
    file_log_level = os.getenv("file_log_level", "INFO")
    # log folder under /app. Ex: /app/log/logfile
    log_path = Path('./log')
    log_path.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger('__name__')
    logger.setLevel(file_log_level)

    logger.debug(f"[Start_worker] file log level: {file_log_level}")
    log_file = 'log'
    file_log = {
        "launch_info": dict(),
        "time_start": str(datetime.datetime.utcnow())
    }

    logger_fh = logging.FileHandler(posixpath.join(log_path, log_file))
    formatter_fh = logging.Formatter('%(message)s')
    logger_fh.setFormatter(formatter_fh)
    logger.addHandler(logger_fh)

    worker_type, worker_config, file_log_config = load_config(file_log)
    file_log["launch_info"]["build_version"] = build_version
    file_log["launch_info"]["video_download_dir"] = worker_config.get("video_download_dir", None)
    file_log["launch_info"]["inbound_queue"] = worker_config.get("inbound_queue", None)
    file_log["launch_info"]["inbound_routing_key"] = worker_config.get("inbound_routing_key", None)
    file_log["launch_info"]["outbound_queue"] = worker_config.get("outbound_queue", None)
    file_log["launch_info"]["outbound_routing_key"] = worker_config.get("outbound_routing_key", None)
    file_log["launch_info"]["in_exchange"] = worker_config.get("in_exchange", None)
    file_log["launch_info"]["exchange"] = worker_config.get("exchange", None)
    file_log["launch_info"]["exchange_type"] = worker_config.get("exchange_type", None)
    file_log["launch_info"]["s3_download_bucket"] = worker_config.get("s3_download_bucket", None)
    file_log["launch_info"]["s3_download_key_prefix"] = worker_config.get("s3_download_key_prefix", None)
    file_log["launch_info"]["s3_upload_bucket"] = worker_config.get("s3_upload_bucket", None)
    file_log["launch_info"]["s3_upload_key_prefix"] = worker_config.get("s3_upload_key_prefix", None)
    file_log["launch_info"]["clean_folder"] = worker_config.get("clean_folder", None)
    file_log["launch_info"]["smart_download"] = worker_config.get("smart_download", None)
    file_log["launch_info"]["rbmq_hosts"] = worker_config.get("rbmq_hosts", None)
    file_log["launch_info"]["rbmq_port"] = worker_config.get("rbmq_port", None)
    file_log["launch_info"]["rbmq_vhost"] = worker_config.get("rbmq_vhost", None)
    file_log["launch_info"]["s3_upload_song_key_prefix"] = worker_config.get("s3_upload_song_key_prefix", None)
    file_log["launch_info"]["audio_bitrate"] = worker_config.get("audio_bitrate", None)
    file_log["launch_info"]["log_file"] = worker_config.get("log_file", None)
    file_log["launch_info"]["logo_file"] = worker_config.get("logo_file", None)
    file_log["launch_info"]["task_interval_sec"] = worker_config.get("task_interval_sec", None)

    logger.info(json.dumps(file_log_config))
    worker = use_worker(worker_type)(**worker_config)
    worker.run()
