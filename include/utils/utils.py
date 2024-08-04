from minio import Minio

MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "miniopassword"
MINIO_IP = "host.docker.internal:9000"

def get_minio_client():
    client = Minio(MINIO_IP, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    return client