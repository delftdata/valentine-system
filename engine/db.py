import os

from minio import Minio
from redis import Redis
from sqlalchemy import create_engine


match_result_db: Redis = Redis(host=os.environ['REDIS_HOST'],
                               port=os.environ['REDIS_PORT'],
                               password=os.environ['REDIS_PASSWORD'],
                               db=0)

insertion_order_db: Redis = Redis(host=os.environ['REDIS_HOST'],
                                  port=os.environ['REDIS_PORT'],
                                  password=os.environ['REDIS_PASSWORD'],
                                  charset="utf-8",
                                  decode_responses=True,
                                  db=1)

verified_match_db: Redis = Redis(host=os.environ['REDIS_HOST'],
                                 port=os.environ['REDIS_PORT'],
                                 password=os.environ['REDIS_PASSWORD'],
                                 charset="utf-8",
                                 decode_responses=True,
                                 db=2)

runtime_db: Redis = Redis(host=os.environ['REDIS_HOST'],
                          port=os.environ['REDIS_PORT'],
                          password=os.environ['REDIS_PASSWORD'],
                          charset="utf-8",
                          decode_responses=True,
                          db=3)

task_result_db: Redis = Redis(host=os.environ['REDIS_HOST'],
                              port=os.environ['REDIS_PORT'],
                              password=os.environ['REDIS_PASSWORD'],
                              db=4)

holistic_job_source_db: Redis = Redis(host=os.environ['REDIS_HOST'],
                                      port=os.environ['REDIS_PORT'],
                                      password=os.environ['REDIS_PASSWORD'],
                                      db=5)

holistic_jobs_completed_tasks_db: Redis = Redis(host=os.environ['REDIS_HOST'],
                                                port=os.environ['REDIS_PORT'],
                                                password=os.environ['REDIS_PASSWORD'],
                                                db=6)

holistic_jobs_total_number_of_tasks_db: Redis = Redis(host=os.environ['REDIS_HOST'],
                                                      port=os.environ['REDIS_PORT'],
                                                      password=os.environ['REDIS_PASSWORD'],
                                                      db=7)

holistic_job_progression_counters: Redis = Redis(host=os.environ['REDIS_HOST'],
                                                 port=os.environ['REDIS_PORT'],
                                                 password=os.environ['REDIS_PASSWORD'],
                                                 db=8)

minio_client: Minio = Minio(f"{os.environ['MINIO_HOST']}:{os.environ['MINIO_PORT']}",
                            access_key=os.environ['MINIO_ACCESS_KEY'],
                            secret_key=os.environ['MINIO_SECRET_KEY'],
                            secure=False)

postgres_engine = create_engine(f"postgresql+psycopg2://"
                                f"{os.environ['POSTGRES_USER']}:"
                                f"{os.environ['POSTGRES_PASSWORD']}@"
                                f"{os.environ['POSTGRES_HOST']}:"
                                f"{os.environ['POSTGRES_PORT']}")
