import boto3
import botocore
from utils import timer

class S3(object):

    def __init__(self, aws_access_key_id, aws_secret_access_key):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.s3 = self.use_s3_client()

    def use_s3_client(self):
        # if (is_test):
        #     print("================================ Running mocked s3 ================================")
        #     return boto3.resource('s3', endpoint_url='http://localhost:4567', aws_access_key_id=aws_access_key_id,
        #                         aws_secret_access_key=aws_secret_access_key)
        # else:
        return boto3.client('s3', aws_access_key_id=self.aws_access_key_id, aws_secret_access_key=self.aws_secret_access_key)

    # def show_object_list(self):
    #     for s3_file in self.use_s3_client(self.aws_access_key_id, self.aws_secret_access_key).Bucket(self.s3_upload_bucket).objects.all():
    #         print("s3_file=", s3_file)
    #         print("s3_file.key=", s3_file.key)

    @timer
    def s3_download_file(self, bucket, key, local_file, logger, retries=10):
        retry = 0
        while True:
            try:
                logger.debug(f"[s3_download_file] download bucket: {bucket}, key: {key}, local_file: {local_file}")
                self.s3.download_file(bucket, key, local_file)
                break
            except botocore.exceptions.ClientError as e:
                logger.debug(f"[s3_download_file] s3 download failed with exception: {e}")
                if retry == retries:
                    raise e
                retry += 1
                logger.debug(f"[s3_download_file] retry #{retry}")

    @timer
    def s3_upload_file(self, bucket, key, upload_file, logger, retries=10, content_type="video/mp4"):
        retry = 0
        while True:
            try:
                logger.debug(f"[s3_upload_file] upload bucket: {bucket}, key: {key}, upload_file: {upload_file}")
                self.s3.upload_file(upload_file, bucket, key, ExtraArgs={"ContentType": content_type})
                break
            except botocore.exceptions.ClientError as e:
                logger.debug(f"[s3_upload_file] s3 uploading failed with exception: {e}")
                if retry == retries:
                    raise e
                retry += 1
                logger.debug(f"[s3_upload_file] retry #{retry}")