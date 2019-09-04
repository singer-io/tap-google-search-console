import json
from urllib.parse import quote
import singer
from singer import metrics, metadata, Transformer, utils
from tap_google_search_console.transform import transform_json

LOGGER = singer.get_logger()
BASE_URL = 'https://www.googleapis.com/webmasters/v3'

def write_schema(catalog, stream_name):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    try:
        singer.write_schema(stream_name, schema, stream.key_properties)
    except OSError as err:
        LOGGER.info('OS Error writing schema for: {}'.format(stream_name))
        raise err


def write_record(stream_name, record, time_extracted):
    try:
        singer.write_record(stream_name, record, time_extracted=time_extracted)
    except OSError as err:
        LOGGER.info('OS Error writing record for: {}'.format(stream_name))
        LOGGER.info('record: {}'.format(record))
        raise err


def get_bookmark(state, stream, site, sub_type, default):
    if (state is None) or ('bookmarks' not in state):
        return default
    return (
        state
        .get('bookmarks', {})
        .get(stream, {})
        .get(site, {})
        .get(sub_type, default)
    )


def write_bookmark(state, stream, site, sub_type, value):
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    if stream not in state['bookmarks']:
        state['bookmarks'][stream] = {}
    if site not in state['bookmarks'][stream]:
        state['bookmarks'][stream][site] = {}
    state['bookmarks'][stream][site][sub_type] = value
    LOGGER.info('Write state for Stream: {}, Site: {}, Type: {}, value: {}'.format(
        stream, site, sub_type, value))
    singer.write_state(state)


def transform_datetime(this_dttm):
    with Transformer() as transformer:
        new_dttm = transformer._transform_datetime(this_dttm)
    return new_dttm


def process_records(catalog, #pylint: disable=too-many-branches
                    stream_name,
                    records,
                    time_extracted,
                    bookmark_field=None,
                    bookmark_type=None,
                    max_bookmark_value=None,
                    last_datetime=None,
                    last_integer=None,
                    parent=None,
                    parent_id=None):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    stream_metadata = metadata.to_map(stream.metadata)

    with metrics.record_counter(stream_name) as counter:
        for record in records:
            # If child object, add parent_id to record
            if parent_id and parent:
                record[parent + '_id'] = parent_id

            # Transform record for Singer.io
            with Transformer() as transformer:
                transformed_record = transformer.transform(
                    record,
                    schema,
                    stream_metadata)
                # Reset max_bookmark_value to new value if higher
                if bookmark_field and (bookmark_field in transformed_record):
                    if (max_bookmark_value is None) or \
                        (transformed_record[bookmark_field] > transform_datetime(max_bookmark_value)):
                        max_bookmark_value = transformed_record[bookmark_field]

                if bookmark_field and (bookmark_field in transformed_record):
                    if bookmark_type == 'integer':
                        # Keep only records whose bookmark is after the last_integer
                        if transformed_record[bookmark_field] >= last_integer:
                            write_record(stream_name, transformed_record, \
                                time_extracted=time_extracted)
                            counter.increment()
                    elif bookmark_type == 'datetime':
                        last_dttm = transform_datetime(last_datetime)
                        bookmark_dttm = transform_datetime(transformed_record[bookmark_field])
                        # Keep only records whose bookmark is after the last_datetime
                        if bookmark_dttm >= last_dttm:
                            # LOGGER.info('record1: {}'.format(record)) # TESTING, comment out
                            write_record(stream_name, transformed_record, \
                                time_extracted=time_extracted)
                            counter.increment()
                else:
                    # LOGGER.info('record2: {}'.format(record)) # TESTING, comment out
                    write_record(stream_name, transformed_record, time_extracted=time_extracted)
                    counter.increment()

        LOGGER.info('Stream: {}, Processed {} records'.format(stream_name, counter.value))
        return max_bookmark_value


# Sync a specific parent or child endpoint.
def sync_endpoint(client, #pylint: disable=too-many-branches
                  catalog,
                  state,
                  start_date,
                  stream_name,
                  site,
                  sub_type,
                  dimensions_list,
                  path,
                  endpoint_config,
                  api_method,
                  pagination,
                  static_params,
                  bookmark_query_field=None,
                  bookmark_field=None,
                  bookmark_type=None,
                  data_key=None,
                  body_params=None,
                  id_fields=None,
                  parent=None,
                  parent_id=None):

    # Get the latest bookmark for the stream and set the last_integer/datetime
    last_datetime = None
    last_integer = None
    max_bookmark_value = None
    if bookmark_type == 'integer':
        last_integer = get_bookmark(state, stream_name, site, sub_type, 0)
        max_bookmark_value = last_integer
    else:
        last_datetime = get_bookmark(state, stream_name, site, sub_type, start_date)
        max_bookmark_value = last_datetime

    # Pagination: loop thru all pages of data
    # Pagination types: none, body, params
    # Each page has an offset (starting value) and a limit (batch size, number of records)
    # Increase the "offset" by the "limit" for each batch.
    # Continue until the "offset" exceeds the total_records.
    offset = 0 # Starting offset value for each batch API call
    limit = 25000 # Batch size; Number of records per API call
    total_records = limit # Initialize total; set to actual total on first API call

    while offset <= total_records:
        if pagination == 'body':
            body = {
                'startRow': offset,
                'rowLimit': limit,
                **body_params # adds in endpoint specific, sort, filter body params
            }
            params = static_params
        elif pagination == 'params':
            params = {
                'startRow': offset,
                'rowLimit': limit,
                **static_params # adds in endpoint specific, sort, filter body params
            }
            body = body_params
        else:
            params = static_params
            body = body_params

        if bookmark_query_field:
            if bookmark_type == 'datetime':
                params[bookmark_query_field] = last_datetime
            elif bookmark_type == 'integer':
                params[bookmark_query_field] = last_integer

        LOGGER.info('Stream: {}, Site: {}, Type: {} - Batch Sync start, Offset: {} {}'.format(
            stream_name,
            site,
            sub_type,
            offset,
            ', Since: {}, '.format(last_datetime) if bookmark_query_field else ''))

        # Squash params to query-string params
        querystring = None
        if params.items():
            querystring = '&'.join(['%s=%s' % (key, value) for (key, value) in params.items()])
        LOGGER.info('URL for Stream: {}, Site: {} ({}): {}/{}{}'.format(
            stream_name,
            site,
            api_method,
            BASE_URL,
            path,
            '?{}'.format(querystring) if querystring else ''))
        if body and not body == {}:
            LOGGER.info('body = {}'.format(body))

        # API request data, endpoint = stream_name passed to client for metrics logging
        data = {}
        if api_method == 'GET':
            data = client.get(
                path=path,
                params=querystring,
                endpoint=stream_name)
        elif api_method == 'POST':
            data = client.post(
                path=path,
                params=querystring,
                endpoint=stream_name,
                data=json.dumps(body))

        # time_extracted: datetime when the data was extracted from the API
        time_extracted = utils.now()
        if not data or data is None or data == {}:
            LOGGER.info('xxx NO DATA xxx')
            return 0 # No data results

        # Transform data with transform_json from transform.py
        transformed_data = [] # initialize the record list

        # Sites endpoint returns a single record dictionary (not a list)
        if stream_name == 'sites':
            data_list = []
            data_list.append(data)
            data_dict = {}
            data_dict[data_key] = data_list
            data = data_dict
        # LOGGER.info('data = {}'.format(data)) # TESTING, comment out
        if data_key in data:
            LOGGER.info('Number of raw data records: {}'.format(len(data[data_key])))
            transformed_data = transform_json(
                data,
                stream_name,
                data_key,
                site,
                sub_type,
                dimensions_list)[data_key]
            LOGGER.info('Number of transformed_data records: {}'.format(len(transformed_data)))
        else:
            LOGGER.info('Number of raw data records: 0')
        # LOGGER.info('transformed_data = {}'.format(transformed_data))  # TESTING, comment out
        if not transformed_data or transformed_data is None:
            LOGGER.info('xxx NO TRANSFORMED DATA xxx')
            return 0 # No data results

        # Process records and get the max_bookmark_value and record_count for the set of records
        max_bookmark_value = process_records(
            catalog=catalog,
            stream_name=stream_name,
            records=transformed_data,
            time_extracted=time_extracted,
            bookmark_field=bookmark_field,
            bookmark_type=bookmark_type,
            max_bookmark_value=max_bookmark_value,
            last_datetime=last_datetime,
            last_integer=last_integer,
            parent=parent,
            parent_id=parent_id)

        # set total_records for pagination
        total_records = offset + len(transformed_data)
        LOGGER.info('total_records: {}, offset: {}, length: {}'.format(
            total_records,
            offset,
            len(transformed_data)))

        # Loop thru parent batch records for each children objects (if should stream)
        # NOT USED FOR THIS TAP; no current endpoints have children
        children = endpoint_config.get('children')
        if children:
            for child_stream_name, child_endpoint_config in children.items():
                should_stream, last_stream_child = should_sync_stream(
                    get_selected_streams(catalog),
                    None,
                    child_stream_name)
                if should_stream:
                    # For each parent record
                    for record in transformed_data:
                        i = 0
                        # Set parent_id
                        for id_field in id_fields:
                            if i == 0:
                                parent_id_field = id_field
                            if id_field == 'id':
                                parent_id_field = id_field
                            i = i + 1
                        parent_id = record.get(parent_id_field)

                        # sync_endpoint for child
                        LOGGER.info('START Syncing Child Stream: {}, Parent Stream: {}, Parent ID: {}, Site: {}, Type: {}'.format(
                                child_stream_name,
                                stream_name,
                                parent_id,
                                site,
                                sub_type))
                        child_path = child_endpoint_config.get('path').format(str(parent_id))
                        child_total_records = sync_endpoint(
                            client=client,
                            catalog=catalog,
                            state=state,
                            start_date=start_date,
                            stream_name=child_stream_name,
                            site=site,
                            sub_type=sub_type,
                            dimensions_list=[],
                            path=child_path,
                            endpoint_config=child_endpoint_config,
                            api_method=child_endpoint_config.get('api_method', 'GET'),
                            pagination=child_endpoint_config.get('pagination', 'none'),
                            static_params=child_endpoint_config.get('params', {}),
                            bookmark_query_field=child_endpoint_config.get('bookmark_query_field'),
                            bookmark_field=child_endpoint_config.get('bookmark_field'),
                            bookmark_type=child_endpoint_config.get('bookmark_type'),
                            data_key=child_endpoint_config.get('data_key', None),
                            body_params=child_endpoint_config.get('body', None),
                            id_fields=child_endpoint_config.get('id_fields'),
                            parent=child_endpoint_config.get('parent'),
                            parent_id=parent_id)

                        LOGGER.info('START Syncing Child Stream: {}, Parent Stream: {}, Parent ID: {}, Site: {}, Type: {}, Total Records: {}'.format(
                                child_stream_name,
                                stream_name,
                                parent_id,
                                site,
                                sub_type,
                                child_total_records))

        # Update the state with the max_bookmark_value for the stream, site, sub_type
        if bookmark_field:
            write_bookmark(state,
                           stream_name,
                           site,
                           sub_type,
                           max_bookmark_value)

        # to_rec: to record; ending record for the batch
        to_rec = offset + limit
        if to_rec > total_records:
            to_rec = total_records

        LOGGER.info('Stream: {}, Site: {}, Type: {} - Synced batch records - {} to {}'.format(
            stream_name,
            site,
            sub_type,
            offset,
            to_rec))
        # Pagination: increment the offset by the limit (batch-size)
        offset = offset + limit

    # Return total_records across all batches
    return total_records


# Review catalog and make a list of selected streams
def get_selected_streams(catalog):
    selected_streams = set()
    for stream in catalog.streams:
        mdata = metadata.to_map(stream.metadata)
        root_metadata = mdata.get(())
        if root_metadata and root_metadata.get('selected') is True:
            selected_streams.add(stream.tap_stream_id)
    return list(selected_streams)


# Currently syncing sets the stream currently being delivered in the state.
# If the integration is interrupted, this state property is used to identify
#  the starting point to continue from.
# Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
def update_currently_syncing(state, stream_name):
    if (stream_name is None) and ('currently_syncing' in state):
        del state['currently_syncing']
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


# Review last_stream (last currently syncing stream), if any,
#  and continue where it left off in the selected streams.
# Or begin from the beginning, if no last_stream, and sync
#  all selected steams.
# Returns should_sync_stream (true/false) and last_stream.
def should_sync_stream(selected_streams, last_stream, stream_name):
    if last_stream == stream_name or last_stream is None:
        if last_stream is not None:
            last_stream = None
        if stream_name in selected_streams:
            return True, last_stream
    return False, last_stream


def sync(client, config, catalog, state):
    if 'start_date' in config:
        start_date = config['start_date']

    selected_streams = get_selected_streams(catalog)
    LOGGER.info('selected_streams: {}'.format(selected_streams))

    if not selected_streams:
        return

    # last_stream = Previous currently synced stream, if the load was interrupted
    last_stream = singer.get_currently_syncing(state)
    LOGGER.info('last/currently syncing stream: {}'.format(last_stream))

    # endpoints: API URL endpoints to be called
    # properties:
    #   <root node>: Plural stream name for the endpoint
    #   path: API endpoint relative path, when added to the base URL, creates the full path
    #   data_key: JSON element containing the records for the endpoint
    #   api_method: GET or POST (default GET)
    #   params: Query, sort, and other endpoint specific parameters
    #   pagination: types are none, body, params (default none)
    #   sub_types: list of sub_types for endpoint looping;
    #        if no sub_types, set to 'sub_types': ['self']
    #   bookmark_query_field: Typically a date-time field used for filtering the query
    #   bookmark_field: Replication key field, typically a date-time, used for filtering the results
    #        and setting the state
    #   bookmark_type: Data type for bookmark, integer or datetime
    #   id_fields: Primary key (and other IDs) - only needed for parent endpoints having children.
    #   children: A collection of child endpoints (where the endpoint path includes the parent id)
    #   parent: On each of the children, the singular stream name for parent element

    endpoints = {
        # 'sites': {
        #     'path': 'sites/{}',
        #     'data_key': 'site_entry',
        #     'api_method': 'GET',
        #     'params': {},
        #     'pagination': 'none',
        #     'sub_types': ['self']
        # },

        # 'sitemaps': {
        #     'path': 'sites/{}/sitemaps',
        #     'data_key': 'sitemap',
        #     'api_method': 'GET',
        #     'params': {},
        #     'pagination': 'none',
        #     'sub_types': ['self']
        # },

        'performance_reports': {
            'path': 'sites/{}/searchAnalytics/query',
            'data_key': 'rows',
            'api_method': 'POST',
            'params': {},
            'bookmark_field': 'date',
            'bookmark_type': 'datetime',
            'pagination': 'body',
            'sub_types': ['web', 'image', 'video']
        }

    }

    # Get current datetime (now_dt_str) for query parameters
    now_dt_str = utils.now().strftime('%Y-%m-%d')

    # For each endpoint (above), determine if the stream should be streamed
    #   (based on the catalog and last_stream), then sync those streams.
    for stream_name, endpoint_config in endpoints.items():
        should_stream, last_stream = should_sync_stream(selected_streams,
                                                        last_stream,
                                                        stream_name)
        if should_stream:
            LOGGER.info('STARTED Syncing: {}'.format(stream_name))
            update_currently_syncing(state, stream_name)
            write_schema(catalog, stream_name)
            endpoint_total = 0
            # Initialize body
            body = endpoint_config.get('body', {})
            # Loop through sites from config site_urls
            site_list = []
            if 'site_urls' in config:
                site_list = config['site_urls'].replace(" ", "").split(",")
            for site in site_list:
                LOGGER.info('STARTED Syncing: {}, Site: {}'.format(stream_name, site))
                site_total = 0
                site_encoded = quote(site, safe='')
                path = endpoint_config.get('path').format(site_encoded)

                # Set dimension_list for performance_reports
                dimensions_list = []
                if stream_name == 'performance_reports':
                    # Create dimensions_list from catalog breadcrumb for stream
                    dimensions_all = ['date', 'country', 'device', 'page', 'query']
                    catalog_dict = catalog.to_dict()
                    for stream in catalog_dict['streams']:
                        if stream['stream'] == 'performance_reports':
                            for entry in stream['metadata']:
                                if entry['metadata']['selected']:
                                    if entry['breadcrumb'] and entry['breadcrumb'] is not None:
                                        for field in entry['breadcrumb']:
                                            if field != 'properties':
                                                if field in dimensions_all:
                                                    dimensions_list.append(field)

                # loop through each sub type
                sub_types = endpoint_config.get('sub_types', [])
                for sub_type in sub_types:
                    if stream_name == 'performance_reports':
                        reports_dttm_str = get_bookmark(
                            state,
                            stream_name,
                            site,
                            sub_type,
                            start_date)
                        reports_dt_str = transform_datetime(reports_dttm_str)[:10]
                        body = {
                            'dimensions': dimensions_list,
                            'searchType': sub_type,
                            'startDate': reports_dt_str,
                            'endDate': now_dt_str
                        }

                    LOGGER.info('START Syncing Stream: {}, Site: {}, Type: {}'.format(
                        stream_name, site, sub_type))
                    total_records = sync_endpoint(
                        client=client,
                        catalog=catalog,
                        state=state,
                        start_date=start_date,
                        stream_name=stream_name,
                        site=site,
                        sub_type=sub_type,
                        dimensions_list=dimensions_list,
                        path=path,
                        endpoint_config=endpoint_config,
                        api_method=endpoint_config.get('api_method', 'GET'),
                        pagination=endpoint_config.get('pagination', 'none'),
                        static_params=endpoint_config.get('params', {}),
                        bookmark_query_field=endpoint_config.get('bookmark_query_field'),
                        bookmark_field=endpoint_config.get('bookmark_field'),
                        bookmark_type=endpoint_config.get('bookmark_type'),
                        data_key=endpoint_config.get('data_key', None),
                        body_params=body,
                        id_fields=endpoint_config.get('id_fields', None))

                    endpoint_total = endpoint_total + total_records
                    site_total = site_total + total_records
                    LOGGER.info('FINISHED Syncing Stream: {}, Site: {}, Type: {}'.format(
                        stream_name, site, sub_type))
                    LOGGER.info('  Records Synced for Type: {}'.format(total_records))
                LOGGER.info('FINISHED Syncing Stream: {}, Site: {}'.format(stream_name, site))
                LOGGER.info('  Records Synced for Site: {}'.format(site_total))
            LOGGER.info('FINISHED Syncing Stream: {}'.format(stream_name))
            LOGGER.info('  Records Synced for Stream: {}'.format(endpoint_total))
            update_currently_syncing(state, None)
