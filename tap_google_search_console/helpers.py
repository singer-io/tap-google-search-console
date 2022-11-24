from typing import Dict, List
import hashlib
import json
import os
import re
from urllib.parse import quote

import singer

LOGGER = singer.get_logger()


def get_abs_path(path: str):
    """Returns absolute path for URL."""
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def encode_and_format_url(url: str, string_format: str) -> str:
    """Encodes the given site_url."""
    return string_format.format(quote(url, safe=""))


def convert(name):
    """Converts a CamelCased word to snake case."""
    regsub = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", regsub).lower()


# Convert keys in json array
def convert_array(arr: List):
    """Converts all the CamelCased dict Keys in a list to snake case Iterated
    through each object recursively and if the object is dict type then
    converts its key to snake case."""
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
def convert_json(data_object: Dict):
    """Converts all the CamelCased Keys in a nested dictionary object to snake
        case."""
    out = {}
    for key in data_object:
        new_key = convert(key)
        if isinstance(data_object[key], dict):
            out[new_key] = convert_json(data_object[key])
        elif isinstance(data_object[key], list):
            out[new_key] = convert_array(data_object[key])
        else:
            out[new_key] = data_object[key]
    return out


def remove_keys_nodes(data_object: Dict, path: str):
    """Remove `keys` node, if exists."""
    idx = 0
    for record in list(data_object[path]):
        if record.get("keys", None):
            data_object[path][idx].pop("keys")
        idx = idx + 1
    return data_object


def hash_data(data):
    """Create MD5 hash key for data element Prepares the project id hash."""
    hash_id = hashlib.md5()
    hash_id.update(repr(data).encode("utf-8"))
    return hash_id.hexdigest()


def denest_key_fields(data_object: Dict, stream_name: str, path: str, dimensions_list: List):
    """Denest keys values list to dimension_list keys"""
    idx = 0
    for record in list(data_object[path]):
        for key in list(record.keys()):
            if isinstance(record[key], list) and key == "keys":
                dim_num = 0
                # Add dimensions_hash_key for performance_report_custom
                if stream_name == "performance_report_custom":
                    dims_md5 = str(hash_data(json.dumps(record[key], sort_keys=True)))
                    data_object[path][idx]["dimensions_hash_key"] = dims_md5
                for dimension in dimensions_list:
                    data_object[path][idx][dimension] = record[key][dim_num]
                    dim_num = dim_num + 1
        idx = idx + 1
    return data_object


def add_site_url_search_type(data_object: Dict, path: str, site: str, key_name: str = "site_url"):
    """Adds site_url or Search_type key, value to each object
        Default of key_name is site_url"""
    idx = 0
    for _ in data_object[path]:
        data_object[path][idx][key_name] = site
        idx = idx + 1
    return data_object


def transform_reports(data_object: Dict, stream_name: str, path: str, site: str, sub_type: str, dimensions_list: List):
    """de-nest keys array to dimension fields and add MD5 hash key for custom report"""
    denested_json = denest_key_fields(data_object, stream_name, path, dimensions_list)
    # remove keys array node
    keyless_json = remove_keys_nodes(denested_json, path)
    return add_site_url_search_type(add_site_url_search_type(keyless_json, path, site), path, sub_type,
                                    key_name="search_type")


def transform_json(data_object: Dict, stream_name: str, path: str = "", site: str = "",
                   sub_type: str = "", dimensions_list: List = None):
    """Run all transforms: convert camelCase to snake_case for field_name keys,
    and stream-specific transforms for sitemaps and performance_reports."""
    dimensions_list = dimensions_list or []
    converted_json = convert_json(data_object)
    if stream_name == "sitemaps":
        return add_site_url_search_type(converted_json, path, site)
    elif stream_name.startswith("performance_report"):
        return transform_reports(converted_json, stream_name, path, site, sub_type, dimensions_list)
    return converted_json

