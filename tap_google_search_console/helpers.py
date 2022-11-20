import os
from urllib.parse import quote
from enum import Enum
import re
import hashlib
import json
import singer

LOGGER = singer.get_logger()


def get_abs_path(path: str):
    """
    Returns absolute path for URL
    """
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def encode_and_format_url(url: str, string_format: str) -> str:
    """
    Encodes the given site_url
    """
    return string_format.format(quote(url, safe=""))


def convert(name):
    """
    Converts a CamelCased word to snake case
    """
    regsub = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', regsub).lower()


# Convert keys in json array
def convert_array(arr):
    """
    Converts all the CamelCased dict Keys in a list to snake case
    Iterated through each object recursively and if the object is dict type then converts its key to snake case
    """
    new_arr = []
    for element in arr:
        if isinstance(element, list):
            new_arr.append(convert_array(element))
        elif isinstance(element, dict):
            new_arr.append(convert_json(element))
        else:
            new_arr.append(element)
    return new_arr


# Convert keys in json
def convert_json(this_json):
    """
    Converts all the CamelCased Keys in a nested dictionary object to snake case
    """
    out = {}
    for key in this_json:
        new_key = convert(key)
        if isinstance(this_json[key], dict):
            out[new_key] = convert_json(this_json[key])
        elif isinstance(this_json[key], list):
            out[new_key] = convert_array(this_json[key])
        else:
            out[new_key] = this_json[key]
    return out



def remove_keys_nodes(this_json, path):
    """
    Remove `keys` node, if exists
    """
    new_json = this_json
    idx = 0
    for record in list(new_json[path]):
        if record.get("keys", None):
            new_json[path][idx].pop("keys")
        idx = idx + 1
    return new_json


def hash_data(data):
    """
    Create MD5 hash key for data element
    Prepares the project id hash
    """
    hash_id = hashlib.md5()
    hash_id.update(repr(data).encode("utf-8"))
    return hash_id.hexdigest()



def denest_key_fields(this_json, stream_name, path, dimensions_list):
    """
    Denest keys values list to dimension_list keys
    """
    new_json = this_json
    idx = 0
    for record in list(new_json[path]):
        for key in list(record.keys()):
            if isinstance(record[key], list) and key == "keys":
                dim_num = 0
                # Add dimensions_hash_key for performance_report_custom
                if stream_name == "performance_report_custom":
                    dims_md5 = str(hash_data(json.dumps(record[key], sort_keys=True)))
                    new_json[path][idx]["dimensions_hash_key"] = dims_md5
                for dimension in dimensions_list:
                    new_json[path][idx][dimension] = record[key][dim_num]
                    dim_num = dim_num + 1
        idx = idx + 1
    return new_json


def add_site_url(this_json, path, site):
    """
    Adds site_url key, value to each object
    """
    new_json = this_json
    idx = 0
    for _ in new_json[path]:
        new_json[path][idx]["site_url"] = site
        idx = idx + 1
    return new_json


def add_search_type(this_json, path, sub_type):
    """
    Adds to `search_type` key, value to each object
    """
    new_json = this_json
    idx = 0
    for _ in new_json[path]:
        new_json[path][idx]["search_type"] = sub_type
        idx = idx + 1
    return new_json


def transform_reports(this_json, stream_name, path, site, sub_type, dimensions_list):
    """
    de-nest keys array to dimension fields and add MD5 hash key for custom report
    """
    denested_json = denest_key_fields(this_json, stream_name, path, dimensions_list)
    # remove keys array node
    keyless_json = remove_keys_nodes(denested_json, path)
    return add_search_type(add_site_url(keyless_json, path, site), path, sub_type)



def transform_json(this_json, stream_name, path="", site="", sub_type="", dimensions_list=None):
    """
    Run all transforms: convert camelCase to snake_case for field_name keys,
    and stream-specific transforms for sitemaps and performance_reports.
    """
    if dimensions_list is None:
        dimensions_list = []
    converted_json = convert_json(this_json)
    if stream_name == "sitemaps":
        return add_site_url(converted_json, path, site)
    elif stream_name.startswith("performance_report"):
        return transform_reports(converted_json, stream_name, path, site, sub_type, dimensions_list)

    else:
        return converted_json

