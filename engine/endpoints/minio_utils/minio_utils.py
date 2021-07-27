from tempfile import gettempdir
from os import path
from flask import Blueprint, Response, abort, jsonify
from werkzeug.utils import secure_filename

from engine.db import minio_client
from engine.data_sources.minio.minio_utils import list_bucket_files
from engine.forms import UploadFileToMinioForm

app_minio_utils = Blueprint('app_minio_utils', __name__)


@app_minio_utils.post('/minio/create_bucket/<bucket_name>')
def create_minio_bucket(bucket_name: str):
    minio_client.make_bucket(bucket_name)
    return Response(f"Bucket {bucket_name} created successfully", status=200)


@app_minio_utils.post('/minio/upload_file/<bucket_name>')
def minio_upload_file(bucket_name: str):
    form = UploadFileToMinioForm()
    if not form.validate_on_submit():
        abort(400, form.errors)
    tmp_dir: str = gettempdir()
    filename = secure_filename(form.resource.data.filename)
    src_file = path.join(tmp_dir, filename)
    form.resource.data.save(src_file)
    minio_client.fput_object(bucket_name, filename, src_file)
    return Response(f"File {filename} uploaded in bucket {bucket_name} successfully", status=200)


@app_minio_utils.get('/minio/get_bucket_files/<bucket_name>')
def minio_get_bucket_files(bucket_name: str):
    folder_contents = list_bucket_files(bucket_name, minio_client)
    return jsonify(folder_contents)
