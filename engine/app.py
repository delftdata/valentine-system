import logging
import os

from engine import app, celery, SYSTEM_RESERVED_MINIO_BUCKETS
from engine.db import minio_client
from engine.endpoints.holistic_matching.atlas import app_matches_atlas
from engine.endpoints.holistic_matching.holistic import app_matches_holistic
from engine.endpoints.holistic_matching.match_results import app_matches_results
from engine.endpoints.holistic_matching.minio import app_matches_minio
from engine.endpoints.holistic_matching.postgres import app_matches_postgres
from engine.endpoints.minio_utils.minio_utils import app_minio_utils
from engine.endpoints.postgres_utils.postgres_utils import app_postgres_utils
from engine.endpoints.valentine.results import app_valentine_results
from engine.endpoints.valentine.valentine import app_valentine
from engine.utils.utils import ValentineJsonEncoder, init_minio_buckets


celery.conf.update(broker_url=f"amqp://{os.environ['RABBITMQ_DEFAULT_USER']}:" 
                              f"{os.environ['RABBITMQ_DEFAULT_PASS']}@" 
                              f"{os.environ['RABBITMQ_HOST']}:" 
                              f"{os.environ['RABBITMQ_PORT']}/",
                   task_serializer='msgpack',
                   accept_content=['msgpack'],
                   result_serializer='msgpack',
                   task_acks_late=True,
                   worker_prefetch_multiplier=2
                   )

app.json_encoder = ValentineJsonEncoder

app.register_blueprint(app_matches_minio)
app.register_blueprint(app_matches_atlas)
app.register_blueprint(app_matches_results)
app.register_blueprint(app_minio_utils)
app.register_blueprint(app_valentine_results)
app.register_blueprint(app_valentine)
app.register_blueprint(app_postgres_utils)
app.register_blueprint(app_matches_postgres)
app.register_blueprint(app_matches_holistic)

init_minio_buckets(minio_client, SYSTEM_RESERVED_MINIO_BUCKETS)


if __name__ != '__main__':
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)


if __name__ == '__main__':
    app.run(debug=False)
