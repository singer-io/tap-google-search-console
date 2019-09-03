import re
import hashlib
import json
import singer

LOGGER = singer.get_logger()

# Convert camelCase to snake_case
def convert(name):
    regsub = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', regsub).lower()


# Convert keys in json array
def convert_array(arr):
    new_arr = []
    for i in arr:
        if isinstance(i, list):
            new_arr.append(convert_array(i))
        elif isinstance(i, dict):
            new_arr.append(convert_json(i))
        else:
            new_arr.append(i)
    return new_arr


# Convert keys in json
def convert_json(this_json):
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


# Remove 'keys' node, if exists
def remove_keys_nodes(this_json, path):
    new_json = this_json
    i = 0
    for record in list(this_json[path]):
        if record.get('keys', None):
            new_json[path][i].pop('keys')
        i = i + 1
    return new_json


# Denest keys values list to dimension_list keys
def denest_key_fields(this_json, path, dimensions_list):
    new_json = this_json
    i = 0
    for record in list(this_json[path]):
        for key in list(record.keys()):
            if isinstance(record[key], list):
                if key == 'keys':
                    dim_num = 0
                    dims_md5 = hashlib.md5(json.dumps(record[key], sort_keys=True)).hexdigest()
                    new_json[path][i]['dimensions_hash_key'] = dims_md5
                    for dimension in dimensions_list:
                        new_json[path][i][dimension] = record[key][dim_num]
                        dim_num = dim_num + 1
        i = i + 1
    return new_json


# Add site_url to results
def add_site_url(this_json, path, site):
    new_json = this_json
    i = 0
    for record in this_json[path]:
        new_json[path][i]['site_url'] = site
        i = i + 1
    return new_json


# Add search_type to results
def add_search_type(this_json, path, sub_type):
    new_json = this_json
    i = 0
    for record in this_json[path]:
        new_json[path][i]['search_type'] = sub_type
        i = i + 1
    return new_json


# convert integer string to integer
def string_to_integer(val):
    try:
        new_val = int(val)
        return new_val
    except ValueError:
        return None


def transform_sitemaps(this_json, path, site):
    # add site_url to results
    new_json = add_site_url(this_json, path, site)
    # convert string numbers to integers
    int_fields_1 = ['errors', 'warnings']
    int_fields_2 = ['submitted', 'indexed']
    i = 0
    for record in list(new_json[path]):
        for int_field in int_fields_1:
            if int_field in record:
                val = record[int_field]
                new_json[path][i][int_field] = string_to_integer(val)
        if 'contents' in record:
            con_num = 0
            for content in list(record['contents']):
                for int_field in int_fields_2:
                    if int_field in content:
                        val = content[int_field]
                        new_json[path][i]['contents'][con_num][int_field] = string_to_integer(val)
                con_num = con_num + 1
        i = i + 1
    return new_json


def transform_reports(this_json, path, site, sub_type, dimensions_list):
    # de-nest keys array to dimension fields
    denested_json = denest_key_fields(this_json, path, dimensions_list)
    # remove keys array node
    keyless_json = remove_keys_nodes(denested_json, path)
    # add site_url and search_type to results
    new_json = add_search_type(add_site_url(keyless_json, path, site), path, sub_type)
    return new_json


# Run all transforms: convert camelCase to snake_case for fieldname keys,
#  and stream-specific transforms for sitemaps and performance_reports.
def transform_json(this_json, stream_name, path, site, sub_type, dimensions_list):
    converted_json = convert_json(this_json)
    if stream_name == 'sitemaps':
        new_json = transform_sitemaps(converted_json, path, site)
    elif stream_name == 'performance_reports':
        new_json = transform_reports(converted_json, path, site, sub_type, dimensions_list)
    else:
        new_json = converted_json
    return new_json
