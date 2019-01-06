import unittest
import time
import boto3
from lambda_function import *
import json


class TestMethods(unittest.TestCase):

	def test_get_project_name_from_s3_url__valid_url__returns_project_name(self):
		# Arrange
		input = "https://s3.amazonaws.com/code-index/prep-output/ProjectX/docroot/js/jquery-1.9.1.js"

		# Act
		project_name = get_project_name_from_s3_url(input)

		# Assert
		self.assertEqual(project_name, "ProjectX")


	def test_get_file_extension_from_s3_url__valid_url__returns_project_name(self):
		# Arrange
		input = "https://s3.amazonaws.com/code-index/prep-output/ProjectX/docroot/js/jquery-1.9.1.js"

		# Act
		project_name = get_file_extension_from_s3_url(input)

		# Assert
		self.assertEqual(project_name, ".js")	


	def tests_format_file_csv_string__valid_input__format_is_correct(self):
		# Arrange

		# Act
		result = format_file_csv_string("https://s3.amazonaws.com/code-index/prep-output/ProjectX/test.java", "int i; return i;")
		print(result)

		# Assert
		self.assertTrue("\"ProjectX\"" in result)
		self.assertTrue("\".java\"" in result)
		self.assertTrue("\"test.java\"" in result)
		self.assertTrue("\"int i; return i;\"" in result)


	def tests_format_file_es_bulk_string__valid_input__format_is_correct(self):
		# Arrange

		# Act
		result = format_file_es_bulk_string("https://s3.amazonaws.com/code-index/prep-output/ProjectX/test.java", "int i; return i;")
		print(result)

		# Assert
		self.assertTrue("\"ProjectX\"" in result)
		self.assertTrue("\".java\"" in result)
		self.assertTrue("\"test.java\"" in result)
		self.assertTrue("\"int i; return i;\"" in result)
		self.assertTrue("\"code-index\"" in result)
		self.assertTrue("\"@timestamp\"" in result)
		self.assertEqual(result.count("\n"), 2)

if __name__ == '__main__':
	unittest.main()		


