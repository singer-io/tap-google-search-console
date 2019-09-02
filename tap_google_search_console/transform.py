import re

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


def remove_keys_nodes(this_json, path):
    new_json = this_json
    result = new_json[path].pop("keys", None)
    return new_json


# Convert custom fields and sets
# Generalize/Abstract custom fields to key/value pairs
def denest_key_fields(this_json, path, dimensions_list):
    new_json = this_json
    i = 0
    for record in this_json[path]:
        for key in record:
            if isinstance(record[key], list):
                if key == 'keys':
                    dim_num = 0
                    for dimension in dimensions_list:
                        new_json[path][i][dimension] = record[key][dim_num] 
                        dim_num = dim_num + 1
        i = i + 1
    return new_json


# Run all transforms: denests _embedded, removes _embedded/_links, and
#  converst camelCase to snake_case for fieldname keys.
def transform_json(this_json, path, dimensions_list):
    converted_json = convert_json(this_json)
    new_json = converted_json
    if path == 'rows':
        denested_json = denest_key_fields(converted_json, path, dimensions_list)
        new_json = remove_keys_nodes(denested_json, path)
    return new_json
