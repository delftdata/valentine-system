import json
import os
from typing import List, Optional, Dict
from flask import abort
from pydantic import ValidationError, BaseModel

from ..algorithms.algorithms import schema_only_algorithms, instance_only_algorithms, schema_instance_algorithms
from ..algorithms.base_matcher import BaseMatcher
from ..data_sources.atlas.atlas_source import AtlasSource


from engine.algorithms import algorithms as module_algorithms


class ValentineBaseModel(BaseModel):
    class Config:
        arbitrary_types_allowed: bool = True


class AtlasPayload(ValentineBaseModel):
    atlas_url: str
    atlas_username: str
    atlas_password: str
    db_types: List[str]
    matching_algorithm: str
    # the maximum number of matches to be given as output
    max_number_matches: int = None
    # increase the request_parallelism and request_chunk_size for faster data ingestion from atlas the default
    # values are low enough to work with a very slow internet connection.
    # request_parallelism is how many parallel requests the framework makes to atlas
    # request_chunk_size is how many entities the framework requests at once
    request_parallelism: int = 8
    request_chunk_size: int = 10
    # if the algorithm params are left empty the defaults will be chosen
    matching_algorithm_params: Optional[Dict[str, object]]


class MinioPayload(ValentineBaseModel):
    table_name: str  # Table name is the Csv file name in the store
    db_name: str  # DB name is the name of the folder that it is under
    matching_algorithm: str
    # the maximum number of matches to be given as output
    max_number_matches: int = None
    # if the algorithm params are left empty the defaults will be chosen
    matching_algorithm_params: Optional[Dict[str, object]]


class PostgresPayload(MinioPayload):
    pass


class ValentineBenchmarkPayload(ValentineBaseModel):
    dataset_name: str
    algorithm_params: List[Dict[str, object]]


class MinioBulkPayload(ValentineBaseModel):
    tables: List[Dict[str, str]]  # The tables in the format [{db_name: ..., table_name: ...}, ...]
    algorithms: List[Dict[str, Optional[Dict[str, object]]]]  # The algorithms to run [{#algorithm_name: {params dict}}]


class HolisticBulkPayload(ValentineBaseModel):
    tables: List[Dict[str, str]]  # dict has keys source, db_name, table_name
    algorithms: List[Dict[str, Optional[Dict[str, object]]]]  # The algorithms to run [{#algorithm_name: {params dict}}]


def validate_matcher(name, args, endpoint):
    """
    Validates the matching algorithm params for early failure in the matching process if something is wrong
    """
    try:
        if name not in schema_only_algorithms + instance_only_algorithms + schema_instance_algorithms:
            abort(400, "The selected algorithm does not exist")
        if endpoint == "atlas" and (name not in schema_only_algorithms
                                    and not (name == "Coma" and
                                             (args is None or len(args) == 0 or args["strategy"] == "COMA_OPT"))):
            abort(400, "The selected algorithm requires data instances which atlas cannot provide")
        get_matcher(name, args)
    except AttributeError:
        abort(400, "The selected algorithm does not exist")
    except TypeError:
        abort(400, "Invalid matching algorithm parameters")


def get_atlas_payload(request_json: dict) -> AtlasPayload:
    try:
        payload = AtlasPayload(**request_json)
    except ValidationError:
        abort(400, "Incorrect payload arguments. Make sure that they contain the correct atlas_url: str, "
                   "atlas_username: str, atlas_password: str, db_types: List[str] and matching_algorithm: str")
    else:
        return payload


def get_valentine_benchmark_payload(request_json: dict) -> ValentineBenchmarkPayload:
    try:
        payload = ValentineBenchmarkPayload(**request_json)
    except ValidationError:
        abort(400, "Incorrect payload arguments. Make sure that they contain the correct dataset_name: str and "
                   "algorithm_params: List[Dict[str, object]]")
    else:
        return payload


def get_minio_bulk_payload(request_json: dict) -> MinioBulkPayload:
    try:
        payload = MinioBulkPayload(**request_json)
        if not payload.tables:
            abort(400, "Empty table list")
        if not payload.algorithms:
            abort(400, "Empty algorithm list")
    except ValidationError:
        abort(400, "Incorrect payload arguments. Make sure that they contain the correct source_tables: "
                   "the source tables in the format [{db_name: ..., table_name: ...}, ...] same for the target tables"
                   "and the algorithms [{#algorithm_name: {params dict}}]")
    else:
        return payload


def get_minio_payload(request_json: dict) -> MinioPayload:
    try:
        payload = MinioPayload(**request_json)
    except ValidationError:
        abort(400, "Incorrect payload arguments. Make sure that they contain the correct table_name: str and "
                   "db_name: str of the table that you want to find the matches.")
    else:
        return payload


def get_postgres_payload(request_json: dict) -> PostgresPayload:
    try:
        payload = PostgresPayload(**request_json)
    except ValidationError:
        abort(400, "Incorrect payload arguments. Make sure that they contain the correct table_name: str and "
                   "db_name: str of the table that you want to find the matches.")
    else:
        return payload


def get_holistic_bulk_payload(request_json: dict) -> HolisticBulkPayload:
    try:
        payload = HolisticBulkPayload(**request_json)
    except ValidationError:
        abort(400, "Incorrect payload arguments. Make sure that they contain the correct table_name: str and "
                   "db_name: str of the table that you want to find the matches.")
    else:
        return payload



def get_atlas_source(payload: AtlasPayload) -> AtlasSource:
    try:
        atlas_source: AtlasSource = AtlasSource(payload.atlas_url, payload.atlas_username, payload.atlas_password,
                                                payload.db_types, payload.request_parallelism,
                                                payload.request_chunk_size)
    except json.JSONDecodeError:
        abort(500, "Couldn't connect to Atlas. Check the given atlas url and credentials. "
                   "If they are correct, it is a network issue")
    except KeyError:
        abort(400, "One or more of the given database types does not exist or has a typo")
    else:
        return atlas_source


def get_matcher(name, args) -> BaseMatcher:
    return getattr(module_algorithms, name)() if args is None else getattr(module_algorithms, name)(**dict(args))


def string_is_int(string: str) -> bool:
    try:
        _ = int(string)
    except ValueError:
        return False
    else:
        return True


def get_ranged_params(splitted: list[str]):
    values = []
    start, step, end = splitted
    if string_is_int(start) and string_is_int(start) and string_is_int(start):
        start, step, end = int(start), int(step), int(end)
    else:
        start, step, end = float(start), float(step), float(end)
    tmp = start
    while tmp <= end:
        values.append(tmp)
        tmp += step
    return values


def get_params_from_str_input(str_input: str):
    try:
        p_variants = str_input.split(',')

        values = []
        for variant in p_variants:
            if variant.lower().islower():
                values.append(variant)
            else:
                splitted = variant.split(':')
                if len(splitted) > 1:
                    values.extend(get_ranged_params(splitted))
                else:
                    if string_is_int(variant):
                        values.append(int(variant))
                    else:
                        values.append(float(variant))
    except ValueError:
        abort(400, 'Invalid parameter input string')
    except AttributeError:
        return [str_input]
    else:
        return values
