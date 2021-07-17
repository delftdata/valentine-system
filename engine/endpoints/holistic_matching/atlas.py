import json
import uuid
from itertools import product

from celery import chord
from flask import abort, request, jsonify, Blueprint

from engine.data_sources.atlas.atlas_source import AtlasSource
from engine.data_sources.atlas.atlas_table import AtlasTable
from engine.data_sources.base_db import BaseDB
from engine.data_sources.base_source import GUIDMissing
from engine.utils.api_utils import AtlasPayload, validate_matcher, get_atlas_payload, get_atlas_source
from engine.celery_tasks.holistic_matching import merge_matches, get_matches_atlas


TABLE_DOES_NOT_EXIST_RESPONSE_STR = "The table does not exist"
EMPTY_TABLE_RESPONSE_STR = "The table does not have any columns"
NO_ATLAS_CONNECTION_RESPONSE = "Couldn't connect to Atlas. This is a network issue, " \
                               "try to lower the request_chunk_size and the request_parallelism in the payload"
GUID_NOT_FOUND_RESPONSE = "This guid does not correspond to any database in atlas! " \
                          "Check if the given database types are correct or if there is a mistake in the guid"

app_matches_atlas = Blueprint('app_matches_atlas', __name__)


@app_matches_atlas.route("/matches/atlas/holistic/<table_guid>", methods=['POST'])
def find_holistic_matches_of_table_atlas(table_guid: str):
    payload: AtlasPayload = get_atlas_payload(request.json)
    validate_matcher(payload.matching_algorithm, payload.matching_algorithm_params, "atlas")
    atlas_src: AtlasSource = get_atlas_source(payload)
    try:
        table: AtlasTable = atlas_src.get_db_table(table_guid)
        if table.is_empty:
            abort(400, EMPTY_TABLE_RESPONSE_STR)
        dbs_tables_guids: list[list[str]] = [x.get_table_str_guids() for x in atlas_src.get_all_dbs().values()]
    except json.JSONDecodeError:
        abort(500, NO_ATLAS_CONNECTION_RESPONSE)
    except (GUIDMissing, KeyError):
        abort(400, GUID_NOT_FOUND_RESPONSE)
    else:
        job_uuid: str = str(uuid.uuid4())
        callback = merge_matches.s(job_uuid, payload.max_number_matches)
        header = [get_matches_atlas.s(payload.matching_algorithm, payload.matching_algorithm_params, *table_combination,
                                      request.json)
                  for table_combination in
                  product([item for sublist in dbs_tables_guids for item in sublist], [table.unique_identifier])]
        chord(header)(callback)
        return jsonify(job_uuid)


@app_matches_atlas.route('/matches/atlas/other_db/<table_guid>/<db_guid>', methods=['POST'])
def find_matches_other_db_atlas(table_guid: str, db_guid: str):
    payload: AtlasPayload = get_atlas_payload(request.json)
    validate_matcher(payload.matching_algorithm, payload.matching_algorithm_params, "atlas")
    atlas_src: AtlasSource = get_atlas_source(payload)
    try:
        table: AtlasTable = atlas_src.get_db_table(table_guid)
        if table.is_empty:
            abort(400, EMPTY_TABLE_RESPONSE_STR)
        db: BaseDB = atlas_src.get_db(db_guid)
    except json.JSONDecodeError:
        abort(500, NO_ATLAS_CONNECTION_RESPONSE)
    except (GUIDMissing, KeyError):
        abort(400, GUID_NOT_FOUND_RESPONSE)
    else:
        job_uuid: str = str(uuid.uuid4())
        callback = merge_matches.s(job_uuid, payload.max_number_matches)
        header = [get_matches_atlas.s(payload.matching_algorithm, payload.matching_algorithm_params, *table_combination,
                                      request.json)
                  for table_combination in product(db.get_table_str_guids(), [table.unique_identifier])]
        chord(header)(callback)
        return jsonify(job_uuid)


@app_matches_atlas.route('/matches/atlas/within_db/<table_guid>', methods=['POST'])
def find_matches_within_db_atlas(table_guid: str):
    payload: AtlasPayload = get_atlas_payload(request.json)
    validate_matcher(payload.matching_algorithm, payload.matching_algorithm_params, "atlas")
    atlas_src: AtlasSource = get_atlas_source(payload)
    try:
        table: AtlasTable = atlas_src.get_db_table(table_guid)
        if table.is_empty:
            abort(400, EMPTY_TABLE_RESPONSE_STR)
        db: BaseDB = atlas_src.get_db(table.db_belongs_uid)
        if db.number_of_tables == 1:
            abort(400, "The given db only contains one table")
    except json.JSONDecodeError:
        abort(500, NO_ATLAS_CONNECTION_RESPONSE)
    except (GUIDMissing, KeyError):
        abort(400, GUID_NOT_FOUND_RESPONSE)
    else:
        # remove the table from the schema so that it doesn't compare against itself
        db.remove_table(table_guid)
        job_uuid: str = str(uuid.uuid4())
        callback = merge_matches.s(job_uuid, payload.max_number_matches)
        header = [get_matches_atlas.s(payload.matching_algorithm, payload.matching_algorithm_params, *table_combination,
                                      request.json)
                  for table_combination in product(db.get_table_str_guids(), [table.unique_identifier])]
        chord(header)(callback)
        return jsonify(job_uuid)
