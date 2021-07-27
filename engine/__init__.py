import os
from celery import Celery
from flask import Flask
from flask_cors import CORS

from engine.db import minio_client
from engine.utils.utils import init_minio_buckets

app = Flask(__name__)

app.config['CELERY_BROKER_URL'] = f"amqp://{os.environ['RABBITMQ_DEFAULT_USER']}:" \
                                  f"{os.environ['RABBITMQ_DEFAULT_PASS']}@" \
                                  f"{os.environ['RABBITMQ_HOST']}:" \
                                  f"{os.environ['RABBITMQ_PORT']}/"


app.config['CELERY_RESULT_BACKEND_URL'] = f"redis://:{os.environ['REDIS_PASSWORD']}@" \
                                          f"{os.environ['REDIS_HOST']}:" \
                                          f"{os.environ['REDIS_PORT']}/0"

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'], backend=app.config['CELERY_RESULT_BACKEND_URL'])
celery.conf.update(app.config)

VALENTINE_METRICS_TO_COMPUTE = {
    "names": ["precision", "recall", "f1_score", "precision_at_n_percent", "recall_at_sizeof_ground_truth",
              "get_spurious_results_at_sizeof_ground_truth"],
    "args": {
        "n": [10, 20, 30, 40, 50, 60, 70, 80, 90]
            }
}
TMP_MINIO_BUCKET = "tmp-folder"
VALENTINE_RESULTS_MINIO_BUCKET = "valentine-results"
VALENTINE_FABRICATED_MINIO_BUCKET = "fabricated-data"
VALENTINE_PLOTS_MINIO_BUCKET = "valentine-plots"

SYSTEM_RESERVED_MINIO_BUCKETS = [TMP_MINIO_BUCKET, VALENTINE_RESULTS_MINIO_BUCKET, VALENTINE_FABRICATED_MINIO_BUCKET,
                                 VALENTINE_PLOTS_MINIO_BUCKET]

init_minio_buckets(minio_client, SYSTEM_RESERVED_MINIO_BUCKETS)

CORS(app)


class ValentineLoadDataError(Exception):
    pass
