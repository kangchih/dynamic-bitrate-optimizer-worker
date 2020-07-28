# Copyright 2018 LangLive All Rights Reserved
import os
import re
import requests
import subprocess
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from utils import timer


@timer
def download_video_from_url(url, local_file, logger, chunk_size=255, retries=10):
    logger.debug(f"[download_video_from_url] Download url: {url}")
    s = requests.Session()
    retries = Retry(total=retries, backoff_factor=1, status_forcelist=[502, 503, 504])
    s.mount('http://', HTTPAdapter(max_retries=retries))
    r = s.get(url)
    with open(local_file, "wb") as f:
        for chunk in r.iter_content(chunk_size=chunk_size):
            if chunk:
                f.write(chunk)
    if not os.path.isfile(local_file):
        logger.error(f"Download MP4 failed: {url}")
        raise FileNotFoundError(local_file)


def parse_s3_url(url):
    """Parse S3 url and return bucket and key (case sensitive)

    :param str url: s3://Bucket/Key
    :rtype (str, str)
    """
    g = re.match(r"^s3:\/\/(?P<bucket_name>[^\/]+)\/(?P<key>.*)", url)
    bucket, key = g.groups()
    return bucket, key

@timer
def minify_raw_video(mp4_file, minify_ops, logger):
    """Modify video file using ffmpeg

    1. Resize
    2. Reduce FPS

    :param str mp4_file: original file path
    :param dict minify_ops: minify operations
    :return: str resized file path
    """
    logger.debug(f"Minify {mp4_file} with ops: {minify_ops}")
    minified = re.sub(".mp4", f".minified.mp4", mp4_file)
    ops_string = ""
    if "dimensions" in minify_ops:
        ops_string += f" -vf scale={minify_ops['dimensions']} "
    if "fps" in minify_ops:
        ops_string += f" -vsync 2 -r {minify_ops['fps']} "
    cmd = f"ffmpeg -y -i {mp4_file} -loglevel panic {ops_string} -c:v libx264 -preset veryslow -c:a copy {minified}"
    logger.debug(f"Minify cmd: {cmd}")
    result = subprocess.call(re.split(r"\s+", cmd))
    logger.debug(f"Minify result: {result}")
    return minified, result


def get_video_info(mp4_path, logger, dimensions=True, fps=True):
    """
    Get information of a mp4 file

    :param str mp4_path: mp4 local path
    :param logger.Logger logger: python logger
    :param bool dimensions: return width & height
    :param bool fps: return fps
    :rtype dict
    """
    keys = []
    if dimensions:
        keys.append("width")
        keys.append("height")
    if fps:
        keys.append("r_frame_rate")

    try:
        logger.debug(f"Get video information: {mp4_path}")
        cmd = f"ffprobe -v error -select_streams v:0 -show_entries stream={','.join(keys)} -of csv=p=0 {mp4_path}"
        logger.debug(f"[get_video_info] cmd={cmd}")
        values = subprocess.check_output(re.split(r"\s+", cmd)).decode("utf-8").strip().split(",")
        logger.debug(f"[get_video_info] values={values}")
        info = dict(zip(keys, values))
        logger.debug(f"[get_video_info] info={info}")
    except Exception as e:
         raise e

    # modify values
    if dimensions:
        info["width"], info["height"] = int(info["width"]), int(info["height"])
    if fps:
        try:
            info["r_frame_rate"] = eval(info["r_frame_rate"])
        except ZeroDivisionError:
            info["r_frame_rate"] = 0
    return info

# Basically the function is the same with get_video_info... in case the response is not compatible with last commit.
def get_video_info_cron(mp4_path, logger, dimensions=True, fps=False):
    """
    Get information of a mp4 file

    :param str mp4_path: mp4 local path
    :param logger.Logger logger: python logger
    :param bool dimensions: return width & height
    :param bool fps: return fps
    :rtype dict
    """
    info = None
    try:
        keys = []
        if dimensions:
            keys.append("width")
            keys.append("height")
        if fps:
            keys.append("r_frame_rate")

        logger.debug(f"[get_video_info_cron] Get video information: {mp4_path}")
        cmd = f"ffprobe -v error -select_streams v:0 -show_entries stream={','.join(keys)} -of csv=p=0 {mp4_path}"
        # logger.debug(f"[get_video_info] cmd={cmd}")
        values = subprocess.check_output(re.split(r"\s+", cmd)).decode("utf-8").strip().split(",")
        logger.debug(f"[get_video_info_cron] values={values}")
        info = dict(zip(keys, values))
        logger.debug(f"[get_video_info_cron] info={info}")

        # modify values
        if dimensions:
            info["width"], info["height"] = int(info["width"]), int(info["height"])
        if fps:
            try:
                info["r_frame_rate"] = eval(info["r_frame_rate"])
            except ZeroDivisionError:
                info["r_frame_rate"] = 0

    except Exception as e:
        logger.debug(f"[Error][get_video_info_cron] Get video information error: {e}")

    finally:
        logger.debug(f"[finally][get_video_info_cron] Get video information end info={info}")
        return info