import boto3


S3 = boto3.resource("s3")


def s3_download(bucket_name, s3_object, local_file):
    bucket = S3.Bucket(bucket_name)

    with open(local_file, "wb") as data:
        bucket.download_fileobj(s3_object, data)
