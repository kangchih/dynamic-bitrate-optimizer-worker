from .dbo import VmafWorker
from .webp import WebpWorker
from .watermark import WatermarkWorker
from .videoinfo import VideoInfoWorker

def use_worker(key):
    key = key.lower()
    available_workers = {
        "dbo": VmafWorker,
        "webp": WebpWorker,
        "watermark": WatermarkWorker,
        "videoinfo": VideoInfoWorker
        # "vmaf_batch": LangVMAFDBOBatchWorker,
    }
    if key not in available_workers.keys():
        raise KeyError(f"[use_worker] worker key {key} is invalid, available keys: {available_workers.keys()}")
    return available_workers[key]