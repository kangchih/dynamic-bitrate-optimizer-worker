import logging
import os
import re
import subprocess
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib

log_level = os.getenv("DBO_LOGLEVEL", "INFO")
log_id = os.getenv("DBO_LOGID", __name__)
logging.basicConfig()
logger = logging.getLogger(log_id)
logger.setLevel(log_level)


def mp4_to_yuv(mp4_path, yuv_path, loglevel="panic", yuv_format="yuv420p", overwrite=False):
    """
    Use ffmpeg command to convert MP4 to YUV (lossless)

    :param mp4_path: (str) mp4 local path
    :param yuv_path: (str) output local path
    :param loglevel: (str, optional) ffmpeg logging level
    :param yuv_format: (str, optional)
    :return: (str) converted YUV path
    """
    try:
        logger.info(f"mp4_to_yuv: {mp4_path}")
        if not os.path.isfile(mp4_path):
            return ""

        if not overwrite and os.path.isfile(yuv_path):
            logger.warning(f"file exists: {yuv_path}")
            return yuv_path

        # run conversion
        cmd = f"ffmpeg -y -i {mp4_path} -loglevel {loglevel} -c:v rawvideo -pix_fmt {yuv_format} {yuv_path}"
        logger.debug(cmd)
        subprocess.call(re.split(r"\s+", cmd))

        # double check
        if os.path.isfile(yuv_path):
            return yuv_path
        else:
            logger.error(f"Conversion {mp4_path} to YUV format is complete, but the file is not found.")
            return ""
    except:
        logger.error(f"Conversion {mp4_path} to YUV format is incomplete.")
        return ""


def crf_compression(mp4_path, comp_mp4_path, crf_value, loglevel="panic"):
    """

    :param mp4_path: (str) mp4 local path
    :param comp_mp4_path: (str) compressed mp4 local path
    :param crf_value: (int)
    :param loglevel:
    :return:
    """
    try:
        # run conversion
        cmd = f"ffmpeg -i {mp4_path} -loglevel {loglevel} -c:v libx264 -preset slow -crf {crf_value} -c:a copy {comp_mp4_path}"
        subprocess.call(re.split(r"\s+", cmd))

        # double check
        if os.path.isfile(comp_mp4_path):
            return True
        else:
            logger.error(f"Compression {mp4_path} with crf {crf_value} is complete, but the file is not found.")
            return False
    except:
        logger.error(f"Compression {mp4_path} with crf {crf_value} is incomplete.")
        return False


def get_dimensions(mp4_path):
    """
    Get height and width of a mp4 file

    :param mp4_path: (str) mp4 local path
    :return: (str) (height,width)
    """
    try:
        logger.info(f"get_dimensions: {mp4_path}")
        cmd = f"ffprobe -v error -select_streams v:0 -show_entries stream=width,height -of csv=p=0 {mp4_path}"
        dimensions = subprocess.check_output(re.split(r"\s+", cmd)).decode("utf-8").strip()
        if len(dimensions.split(",")) == 2:
            return dimensions.split(",")
        else:
            logger.error(f"wrong dimension parsed {mp4_path}: {dimensions}")
            return ""
    except:
        logger.error(f"Can'r get dimensions of {mp4_path}.")
        return ""


def timer(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        return result, f"{te - ts:.3f}"

    return timed


def parse_mix_range(s):
    r = []
    for i in s.split(','):
        if '-' not in i:
            r.append(int(i))
        else:
            l, h = map(int, i.split('-'))
            r += range(l, h + 1)
    return r


def get_rbmq_urls(hosts='localhost, 123.0.1.1', port=5672, user='guest', pwd='guest'):
    r = []
    hosts = re.sub(r"\s+", "", hosts)
    hList = hosts.split(",")
    if (len(hList) > 0):
        for host in hList:
            r.append(f"""amqp://{user}:{pwd}@{host}:{port}""")
    else:
        raise ValueError("rbmq host is not set!")
    return (r[0], r[1:])

def send_err_email(id, worker_type='dbo'):
    try:
        sender = 'imapp.error.report@gmail.com'
        passwd = '9ijn7Ujm'
        receivers = ['kang.kuo@langlive.com']

        emails = [elem.strip().split(',') for elem in receivers]
        msg = MIMEMultipart()
        msg['Subject'] = f"[{worker_type}][{id}] Error"
        msg['From'] = sender
        msg['To'] = ','.join(receivers)
        part = MIMEText("THIS IS AN AUTOMATED MESSAGE - PLEASE DO NOT REPLY DIRECTLY TO THIS EMAIL")
        msg.attach(part)
        smtp = smtplib.SMTP("smtp.gmail.com:587")
        smtp.ehlo()
        smtp.starttls()
        smtp.login(sender, passwd)
        smtp.sendmail(msg['From'], emails, msg.as_string())
    except Exception as e:
        print(f"Send email error: {e}")