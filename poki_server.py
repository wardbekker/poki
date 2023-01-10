# this file will launch a http server that listens to loki logs push requests
# and writes the logs to a partitioned parquet dataset
# it also supports basic logql queries

import random
import time as t

from flask import Flask
from flask import request, jsonify
app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.compute as pc

import logql_parser as plq
from snappy import snappy
import logproto_pb2 as logproto


HTTP_PORT = 3100

# keep a global buffer of unflushed arrow tables
not_flushed_tables = []

# depends on protocol buffer for loki line protocol
# compile with: protoc --proto_path . --python_out=. gogo.proto logproto.proto

if __name__ == '__main__':
    app.run(host='localhost', port=HTTP_PORT, debug=True)

# example parse result 
# {'type': 'query', 'criteria': [('filename', '/var/log/install.log'), ('job', 'varlogs')], 'exact_string_search': 'Ward'}
def execute_query(parsed_logql, limit, direction, time):
    # loop over criteria and create a pyarrow expression for it
    filter = None
    result_tables = []

    for criterion in parsed_logql['criteria']:
        key = criterion[0]
        value = criterion[1]
        expression = (ds.field('stream_' + key) == pc.scalar(value))
        if filter is None:
            filter = expression
        else:
            filter = filter & expression
    
    dataset = ds.dataset('promtail_dataset_test', format='parquet', partitioning=["year", "month", "day", "chunk_id"])

    for batch in dataset.to_batches(filter=filter):
        # check if the logline column has any row that matches the substring Googlebot
        filtered = pa.Table.from_batches([batch]).filter(pc.match_substring(pc.field('logline'), parsed_logql['exact_string_search']))
        result_tables.append(filtered)

    return pa.concat_tables(result_tables).sort_by('ts')



# implement a loki labels endpoint. currently returning sample data
@app.route('/loki/api/v1/labels', methods=['GET'])
def get_loki_labels():
    return jsonify({
        "status": "success",
        "data": [
            "filename",
            "job"
        ]
    }) 

# implement a loki labels  values endpoint. currently returning sample data
@app.route('/loki/api/v1/label/<label>/values', methods=['GET'])
def get_loki_label_values(label):
    _start = request.args.get('start', None)
    _end = request.args.get('end', None)

    return jsonify({
        "status": "success",
        "data": [
            "/var/log/install.log",
            "other.log"
        ]
    })

# implement a loki query endpoint
@app.route('/loki/api/v1/query', methods=['GET'])
def query_loki_logs():
    # parse the query parameters
    # https://grafana.com/docs/loki/latest/api/#query
    #
    # query: the logql query expression
    # limit: the maximum number of log lines to return
    # direction: the direction to query (forwards or backwards)
    # time: The evaluation time for the query as a unix timestamp in seconds
    args = request.args
    query = args.get('query', None)
    limit = args.get('limit', 100)
    direction = args.get('direction', 'forwards')
    time = args.get('time', int(t.perf_counter() * 1000000000))

    # example query {filename="/var/log/install.log", job="varlogs"} |= "Ward" 
    parsed_logql = plq.parse_logql(query)
    res = execute_query(parsed_logql, limit, direction, time)

    streams = dict(parsed_logql['criteria'])
    values = []
    if res.num_rows < int(limit):
        limit = res.num_rows
        
    for i in range(0, int(limit)):
        values.append([str(res["ts"][i].value), res["logline"][i].as_py()])

    # return a sample 200 response as json for a loki query
    json =  jsonify({
        "status": "success",
        "data": {
            "resultType": "streams",
            "result": [
                {
                    "stream": streams,
                    "values": values
                }
            ]
        }
    })
    return json


# handle a post request to the loki api
@app.route('/loki/api/v1/push', methods=['POST'])
def receive_loki_logs():
    assert(request.content_type == "application/x-protobuf")
    # snappy decode the request data
    data = snappy.decompress(request.data)

    # decode the protobuf
    # https://github.com/grafana/loki/blob/main/pkg/logproto/logproto.proto

    push = logproto.PushRequest.FromString(data)

    schema = pa.schema([
        pa.field("chunk_id", pa.int32()),
        pa.field("year", pa.int16()),
        pa.field("month", pa.int16()),
        pa.field("day", pa.int16()),
        pa.field("ts", pa.timestamp('ns')),
        pa.field("logline", pa.string())
    ])

    # generate a random chunk id int
    chunk_id = random.randint(0, 65535)

    # for each stream in push request
    # parse the labels as a json object

    for stream in push.streams:
        # labels are defined as {filename="/var/log/install.log", job="varlogs"}
        # so we need to parse them
        # remote the curly braces from the string
        labels = stream.labels[1:-1]
        # split the string on the comma
        labels = labels.split(",")
        # for each label
        for label in labels:
            # split the label on the equals sign
            label = label.split("=")
            # add the label to the schema if not yet exists
            if "stream_" + label[0].strip() not in schema.names:
                schema = schema.append(pa.field("stream_" + label[0].strip(), pa.string()))
    
    record_batches = []
    
    # for each stream in push request
    for stream in push.streams:
        column_names = ["chunk_id", "year", "month", "day", "ts", "logline"]

        labels = stream.labels[1:-1].split(",")

        # TODO: do some smart calculations for year, month, day
        record_arrays = [
                pa.array([chunk_id] * len(stream.entries), type=pa.int32()),
                pa.array([entry.timestamp.ToDatetime().year for entry in stream.entries], type=pa.int16()),             
                pa.array([entry.timestamp.ToDatetime().month for entry in stream.entries], type=pa.int16()),             
                pa.array([entry.timestamp.ToDatetime().day for entry in stream.entries], type=pa.int16()),             
                pa.array([entry.timestamp.ToNanoseconds() for entry in stream.entries], type=pa.timestamp('ns')), 
                pa.array([entry.line for entry in stream.entries], type=pa.string()),
            ]

        for label in labels:
            label = label.split("=")
            record_arrays.append(pa.array([label[1].strip()[1:-1]] * len(stream.entries), type=pa.string()))
            column_names.append("stream_" + label[0].strip())


        record_batches.append(pa.RecordBatch.from_arrays(record_arrays, names=column_names))

            # print the log line
            #print (entry.timestamp, entry.line)
            # print (entry)

    not_flushed_tables.append(pa.Table.from_batches(record_batches, schema=schema))

    if len(not_flushed_tables) > 20:
        # write the tables to a parquet dataset
        print("writing to dataset")
        ds.write_dataset(pa.concat_tables(not_flushed_tables), 'promtail_dataset_test', format='parquet', existing_data_behavior="overwrite_or_ignore", schema=schema, partitioning=["year", "month", "day", "chunk_id"])
        not_flushed_tables.clear()
    return jsonify(isError= False,
                    message= "Success",
                    statusCode= 200),200
