import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def get_entity_with_guid(url: str, auth: tuple, guid: str) -> dict:
    return requests.get(url + '/api/atlas/v2/entity/guid/' + guid,
                        auth=auth,
                        verify=False).json()


def get_bulk_entities(url: str, auth: tuple, guid_list: list) -> dict:
    return requests.get(url + '/api/atlas/v2/entity/bulk',
                        params={'guid': guid_list},
                        auth=auth,
                        timeout=None,
                        verify=False).json()
