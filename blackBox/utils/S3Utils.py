import boto3
import tempfile

def getCert(key):
    s3 = boto3.resource('s3')
    with tempfile.NamedTemporaryFile(dir="/tmp", delete=False) as f:
        s3.meta.client.download_fileobj('run-adoya-storage', key, f)
        return f.name
