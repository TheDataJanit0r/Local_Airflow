import mimetypes
import boto3
import os
import logging
from botocore.exceptions import ClientError


def upload_file_to_s3(source_dir, filename, bucket_name, bucket_object=None, client=None):
    if not client:
        client = boto3.client(
            's3',
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY")
        )
    try:
        local_filepath = os.path.join(source_dir, filename)
        key = os.path.join(bucket_object, filename) if bucket_object else filename
        client.upload_file(local_filepath, bucket_name, key)
        logging.info('Uploaded file {} to bucket {} key {}'.format(
            local_filepath, bucket_name, key))
    except ClientError as e:
        logging.error(e)


def upload_files_to_s3(source_dir, bucket_name, bucket_object=None):
    client = boto3.client(
        's3',
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY")
    )
    for filename in os.listdir(source_dir):
        logging.info(f"Uploading {filename} to s3")
        try:
            upload_file_to_s3(
                source_dir=source_dir,
                filename=filename,
                bucket_name=bucket_name,
                bucket_object=bucket_object,
                client=client,
            )
        except ClientError as e:
            logging.error(e)


def download_file_from_s3(output_dir, filename, bucket_name, bucket_object=None, client=None):
    """
    Possible bug with bucket_object specified, need further testing.
    Use download_files_from_s3_bucket instead
    """
    if not client:
        client = boto3.client(
            's3',
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            region_name="us-east-1"
        )
    try:
        local_filepath = os.path.join(output_dir, filename)
        key = os.path.join(bucket_object, filename) if bucket_object else filename
        client.download_file(bucket_name, key, local_filepath)
        logging.info('Downloaded file {} from bucket {}'.format(
            local_filepath, bucket_name))
    except ClientError as e:
        logging.error(e)


def download_files_from_s3(output_dir, bucket_name, bucket_object=None):
    """
    Possible bug with bucket_object specified in download_file_from_s3 function,
    need further testing.
    Use download_files_from_s3_bucket instead
    """
    logging.info('Downloading files from s3')
    client = boto3.client(
        's3',
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name="us-east-1"
    )
    bucket_objects = client.list_objects(Bucket=bucket_name)['Contents']
    if bucket_object:
        bucket_objects = [
            obj['Key'] for obj in bucket_objects
            if obj['Key'].startswith(bucket_object)
        ]
    for obj in bucket_objects:
        download_file_from_s3(
            output_dir=output_dir,
            filename=obj,
            bucket_name=bucket_name,
            client=client,
            bucket_object=bucket_object
        )


def download_files_from_s3_bucket(bucket_name, bucket_object, output_dir=None):
    """
    Iterate through bucket structure and replicate it in local
    """
    logging.info('Downloading files from s3')
    s3 = boto3.resource(
        's3',
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name="us-east-1"
    )
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=bucket_object):
        target = (
            obj.key
            if output_dir is None
            else os.path.join(output_dir, os.path.relpath(obj.key, bucket_object))
        )
        if not os.path.exists(os.path.dirname(target)):
            os.makedirs(os.path.dirname(target))
        if obj.key[-1] == '/':
            continue
        logging.info(f"Downloading file: {obj.key}")
        bucket.download_file(obj.key, target)


def upload_dir_to_s3_replicate_structure(dir_path, bucket_name, bucket_object=None):
    s3 = boto3.resource(
        's3',
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_REGION_NAME")
    )
    bucket = s3.Bucket(bucket_name)
    for subdir, dirs, files in os.walk(dir_path):
        for file in files:
            full_path = os.path.join(subdir, file)
            file_mime = mimetypes.guess_type(file)[0] or 'binary/octet-stream'
            with open(full_path, 'rb') as data:
                if bucket_object is not None:
                    key = os.path.join(bucket_object, full_path[len(dir_path)+1:])
                else:
                    key = full_path[len(dir_path)+1:]
                logging.info(f"Uploading file to s3 {key}")
                bucket.put_object(Key=key, Body=data, ContentType=file_mime)


def delete_file_from_s3(bucket_name, file_name, bucket_object=None):
    client = boto3.resource(
        's3',
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_REGION_NAME")
    )
    key = '{}/{}'.format(bucket_object, file_name) if bucket_object else file_name
    client.Object(bucket_name, key).delete()


def delete_files_from_s3(bucket_name, files_list, bucket_object=None):
    for file_name in files_list:
        logging.info(f"Deleting file {file_name}")
        delete_file_from_s3(
            bucket_name=bucket_name,
            file_name=file_name,
            bucket_object=bucket_object
        )
