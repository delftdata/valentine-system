import gzip
import json

from flask import Response, jsonify, Blueprint

from engine.db import match_result_db, insertion_order_db, verified_match_db, runtime_db, holistic_job_source_db

app_matches_results = Blueprint('app_matches_results', __name__)

JOB_DOES_NOT_EXIST_RESPONSE_STR = "Job does not exist"


@app_matches_results.get('/results/finished_jobs')
def get_finished_jobs():
    return jsonify(insertion_order_db.lrange('insertion_ordered_ids', 0, -1))


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
