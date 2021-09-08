from celery import Celery
from flask import Flask
from flask_cors import CORS

app = Flask(__name__)

celery = Celery(app.name)

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

CORS(app)


class ValentineLoadDataError(Exception):
    pass
