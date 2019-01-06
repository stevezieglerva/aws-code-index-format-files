import boto3
from LocalTime import *
import json


def stream_firehose_event(firehose_name, event_data):
	event_data = add_timestamps_to_event(event_data)
	response = stream_firehose_string(firehose_name, json.dumps(event_data))
	return response

def stream_firehose_string(firehose_name, string_data):
	firehose = boto3.client("firehose")
	record = {"Data": string_data}
	response = firehose.put_record(DeliveryStreamName=firehose_name, Record=record)
	return response

def add_timestamps_to_event(event_data):
	local_time = LocalTime()
	if "@timestamp" not in event_data:
		event_data["@timestamp"] = local_time.get_utc_timestamp()
	if "@timestamp_local" not in event_data:
		event_data["@timestamp_local"] = local_time.get_local_timestamp()
	return event_data
