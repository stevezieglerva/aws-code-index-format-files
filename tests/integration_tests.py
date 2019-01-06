import unittest
import time
import boto3
from lambda_function import *
import json


class TestMethods(unittest.TestCase):

	def test_format_file__valid_input__stream_results_created(self):
		# Act
		result = format_files("https://s3.amazonaws.com/code-index/prep-output/ProjectX/test.java", "int i; return i;")
		print(json.dumps(result, indent=3))
		# Assert
		self.assertTrue("RecordId" in result["csv"])
		self.assertTrue("RecordId" in result["es"])

if __name__ == '__main__':
	unittest.main()		


