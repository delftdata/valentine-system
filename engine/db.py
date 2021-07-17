import os

from minio import Minio
from redis import Redis

match_result_db: Redis = Redis(host=os.environ['REDIS_HOST'],
                               port=os.environ['REDIS_PORT'],
                               password=os.environ['REDIS_PASSWORD'],
                               db=1)

insertion_order_db: Redis = Redis(host=os.environ['REDIS_HOST'],
                                  port=os.environ['REDIS_PORT'],
                                  password=os.environ['REDIS_PASSWORD'],
                                  charset="utf-8",
                                  decode_responses=True,
                                  db=2)

verified_match_db: Redis = Redis(host=os.environ['REDIS_HOST'],
                                 port=os.environ['REDIS_PORT'],
                                 password=os.environ['REDIS_PASSWORD'],
                                 charset="utf-8",
                                 decode_responses=True,
                                 db=3)

runtime_db: Redis = Redis(host=os.environ['REDIS_HOST'],
                          port=os.environ['REDIS_PORT'],
                          password=os.environ['REDIS_PASSWORD'],
                          charset="utf-8",
                          decode_responses=True,
                          db=4)

task_result_db: Redis = Redis(host=os.environ['REDIS_HOST'],
                              port=os.environ['REDIS_PORT'],
                              password=os.environ['REDIS_PASSWORD'],
                              db=5)

minio_client: Minio = Minio(f"{os.environ['MINIO_HOST']}:{os.environ['MINIO_PORT']}",
                            access_key=os.environ['MINIO_ACCESS_KEY'],
                            secret_key=os.environ['MINIO_SECRET_KEY'],
                            secure=False)
