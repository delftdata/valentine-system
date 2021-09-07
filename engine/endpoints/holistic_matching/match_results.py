import gzip
import json

from flask import Response, jsonify, Blueprint

from engine.db import match_result_db, insertion_order_db, verified_match_db, runtime_db, holistic_job_source_db, \
    holistic_jobs_total_number_of_tasks_db, holistic_job_progression_counters, holistic_job_uncompleted_tasks_queue
from engine.celery_tasks.holistic_matching import merge_matches, restart_failed_holistic_tasks

app_matches_results = Blueprint('app_matches_results', __name__)

JOB_DOES_NOT_EXIST_RESPONSE_STR = "Job does not exist"


@app_matches_results.get('/results/finished_jobs')
def get_finished_jobs():
    finished_jobs = []
    for started_job_id in insertion_order_db.lrange('insertion_ordered_ids', 0, -1):
        if match_result_db.exists(started_job_id):
            finished_jobs.append(started_job_id)
        elif holistic_jobs_total_number_of_tasks_db.get(started_job_id) \
                == holistic_job_progression_counters.get(started_job_id):
            merge_matches.s(started_job_id).apply_async()
            finished_jobs.append(started_job_id)
    return jsonify(finished_jobs)


@app_matches_results.get('/results/get_jobs_with_progress')
def get_jobs_with_progress():
    jobs = {}
    for job_id in insertion_order_db.lrange('insertion_ordered_ids', 0, -1):
        total = holistic_jobs_total_number_of_tasks_db.get(job_id)
        completed = holistic_job_progression_counters.get(job_id)
        if total == completed and not match_result_db.exists(job_id):
            merge_matches.s(job_id).apply_async()
        jobs[job_id] = f"{completed}/{total}"
    return jsonify(jobs)


@app_matches_results.post('/results/restart_failed_tasks/<job_id>')
def restart_failed_tasks(job_id: str):
    if holistic_job_uncompleted_tasks_queue.hlen(job_id) == 0:
        return Response("Job does not have any failed tasks", status=200)
    restart_failed_holistic_tasks.s(job_id).apply_async()
    return Response("Restart failed tasks success", status=200)


@app_matches_results.post('/results/force_merge/<job_id>')
def force_merge(job_id: str):
    if match_result_db.exists(job_id):
        return Response("Merge already completed", status=200)
    merge_matches.s(job_id).apply_async()
    return Response("Force merge success", status=200)


@app_matches_results.get('/results/job_progress/<job_id>')
def get_job_progress(job_id: str):
    total = holistic_jobs_total_number_of_tasks_db.get(job_id)
    completed = holistic_job_progression_counters.get(job_id)
    return Response(f"{completed}/{total}", status=200)


@app_matches_results.get('/results/job_results/<job_id>')
def get_job_results(job_id: str):
    results = json.loads(gzip.decompress(match_result_db.get(job_id)))
    if results is None:
        return Response(JOB_DOES_NOT_EXIST_RESPONSE_STR, status=400)
    sources = json.loads(holistic_job_source_db.get(job_id))
    return jsonify({"results": results, "sources": sources})


@app_matches_results.get('/results/download_job_results/<job_id>')
def download_job_results(job_id: str):
    results = json.loads(gzip.decompress(match_result_db.get(job_id)))
    if results is None:
        return Response(JOB_DOES_NOT_EXIST_RESPONSE_STR, status=400)
    sources = json.loads(holistic_job_source_db.get(job_id))
    output = json.dumps({"results": results, "sources": sources},
                        indent=2)
    return Response(output,
                    headers={'Content-Type': 'application/json',
                             'Content-Disposition': 'attachment; filename=%s;' % job_id},
                    status=200)


@app_matches_results.get('/results/job_runtime/<job_id>')
def get_job_runtime(job_id: str):
    results = runtime_db.get(job_id)
    if results is None:
        return Response(JOB_DOES_NOT_EXIST_RESPONSE_STR, status=400)
    return jsonify(json.loads(results))


@app_matches_results.post('/results/save_verified_match/<job_id>/<index>')
def save_verified_match(job_id: str, index: int):
    results = match_result_db.get(job_id)
    if results is None:
        return Response(JOB_DOES_NOT_EXIST_RESPONSE_STR, status=400)
    ranked_list: list = json.loads(gzip.decompress(results))
    try:
        to_save = ranked_list.pop(int(index))
    except IndexError:
        return Response("Match does not exist", status=400)
    verified_matches = [json.loads(x) for x in verified_match_db.lrange('verified_matches', 0, -1)]
    match_result_db.set(job_id, gzip.compress(json.dumps(ranked_list).encode('gbk')))
    if to_save in verified_matches:
        return Response("Match already verified", status=200)
    verified_match_db.rpush('verified_matches', json.dumps(to_save))
    return Response("Matched saved successfully", status=200)


@app_matches_results.post('/results/discard_match/<job_id>/<index>')
def discard_match(job_id: str, index: int):
    results = match_result_db.get(job_id)
    if results is None:
        return Response(JOB_DOES_NOT_EXIST_RESPONSE_STR, status=400)
    ranked_list: list = json.loads(gzip.decompress(results))
    try:
        ranked_list.pop(int(index))
    except IndexError:
        return Response("Match does not exist", status=400)
    match_result_db.set(job_id, gzip.compress(json.dumps(ranked_list).encode('gbk')))
    return Response("Matched discarded successfully", status=200)


@app_matches_results.post('/results/delete_job/<job_id>')
def delete_job(job_id: str):
    match_result_db.delete(job_id)
    insertion_order_db.lrem('insertion_ordered_ids', 1, job_id)
    return Response("Job discarded successfully", status=200)


@app_matches_results.get('/results/verified_matches')
def get_verified_matches():
    verified_matches = [json.loads(x) for x in verified_match_db.lrange('verified_matches', 0, -1)]
    return jsonify(verified_matches)
