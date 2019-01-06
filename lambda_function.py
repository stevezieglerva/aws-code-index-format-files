import boto3
import time
import datetime
import logging
import structlog
import os
import json
import sys
from S3TextFromLambdaEvent import *
from firehose_helpers import *

def lambda_handler(event, context):
	try:
		aws_request_id = ""
		aws_request_id = ""
		if context is not None:
			aws_request_id = context.aws_request_id

		print("Started")
		if "text_logging" in os.environ:
			log = structlog.get_logger()
		else:
			log = setup_logging("aws-code-index-format-files", event, aws_request_id)

		print("Started")

		s3 = boto3.resource("s3")
		file_refs = get_files_from_s3_lambda_event(event)
		file_text = get_file_text_from_s3_file_urls(file_refs, s3)

		for file in file_text:
			text = file_text[file]
			format_files(file, text)

		print("Finished")

	except Exception as e:
		print("Exception: "+ str(e))
		raise(e)
		return {"msg" : "Exception"}

	return {"msg" : "Success"}


def format_files(file, text):
	csv_line = format_file_csv_string(file, text)
	csv_response = stream_firehose_string("code-index-files-csv", csv_line)
	es_bulk_line = format_file_es_bulk_string(file, text)
	es_response = stream_firehose_string("code-index-files-es-bulk", es_bulk_line)
	return {"csv" : csv_response, "es" : es_response}

def format_file_csv_string(file, text):
	project = get_project_name_from_s3_url(file)
	raw_filename = get_filename_from_s3_url(file)
	file_ext = get_file_extension_from_s3_url(file)
	
	quoted_filename = "\"" + file + "\""
	quoted_text = "\"" + text + "\""
	quoted_project =  "\"" + project + "\""
	quoted_raw_filename =  "\"" + raw_filename + "\""
	quoted_file_ext =  "\"" + file_ext + "\""
	csv_line = "{}, {}, {}, {}, {}\n".format(quoted_filename, quoted_text, quoted_project, quoted_raw_filename, quoted_file_ext)
	return csv_line

def format_file_es_bulk_string(file, text):
	project = get_project_name_from_s3_url(file)
	raw_filename = get_filename_from_s3_url(file)
	file_ext = get_file_extension_from_s3_url(file)

	index_header = "{\"index\": {\"_index\": \"code-index\", \"_type\": \"doc\"}}"
	index_data = {"filename" : file, "file_text" : text, "raw_filename" : raw_filename, "file_extension" : file_ext, "project" : project}
	index_data = add_timestamps_to_event(index_data)
	index_data_string = index_header + "\n" + json.dumps(index_data) + "\n"
	return index_data_string


def get_project_name_from_s3_url(s3_url):
	#https://s3.amazonaws.com/code-index/prep-output/ProjectX/docroot/js/jquery-1.9.1.js
	url_parts = urlparse(s3_url)
	path_parts = url_parts.path.split('/')
	return path_parts[3]

def get_filename_from_s3_url(s3_url):
	#https://s3.amazonaws.com/code-index/prep-output/ProjectX/docroot/js/jquery-1.9.1.js
	path_parts = s3_url.split('/')
	filename = path_parts[len(path_parts) - 1]
	return filename

def get_file_extension_from_s3_url(s3_url):
	#https://s3.amazonaws.com/code-index/prep-output/ProjectX/docroot/js/jquery-1.9.1.js
	path_parts = s3_url.split('/')
	filename = path_parts[len(path_parts) - 1]
	extension = os.path.splitext(filename)[1]
	return extension	

def setup_logging(lambda_name, lambda_event, aws_request_id):
	logging.basicConfig(
		format="%(message)s",
		stream=sys.stdout,
		level=logging.INFO
	)
	structlog.configure(
		processors=[
			structlog.stdlib.filter_by_level,
			structlog.stdlib.add_logger_name,
			structlog.stdlib.add_log_level,
			structlog.stdlib.PositionalArgumentsFormatter(),
			structlog.processors.TimeStamper(fmt="iso"),
			structlog.processors.StackInfoRenderer(),
			structlog.processors.format_exc_info,
			structlog.processors.UnicodeDecoder(),
			structlog.processors.JSONRenderer()
		],
		context_class=dict,
		logger_factory=structlog.stdlib.LoggerFactory(),
		wrapper_class=structlog.stdlib.BoundLogger,
		cache_logger_on_first_use=True,
	)
	log = structlog.get_logger()
	log = log.bind(aws_request_id=aws_request_id)
	log = log.bind(lambda_name=lambda_name)
	log.critical("started", input_events=json.dumps(lambda_event, indent=3))

	return log
