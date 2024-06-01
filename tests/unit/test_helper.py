import os
import shutil
import sys
import unittest
import yaml

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, arrays_zip, explode
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from unittest.mock import patch, mock_open

from app.helper import refactor_csv_file, get_app_config, transform_df


class TestHelper(unittest.TestCase):

    @patch('os.en'
           'viron', {'TMP_DIR': '/mock/tmp', 'OUTPUT_DIR': '/mock/output'})
    @patch('os.listdir')
    @patch('shutil.copy2')
    def test_refactor_csv_file(self, mock_copy2, mock_listdir):
        # Mock the list of files in the source directory
        mock_listdir.return_value = ['part-00000.csv', 'part-00001.csv']

        # Call the function
        refactor_csv_file('input.csv')

        # Define expected calls to shutil.copy2
        expected_src_file_1 = os.path.join('/mock/tmp', 'input.csv', 'part-00000.csv')
        expected_dst_file_1 = os.path.join('/mock/output', 'input.csv')

        expected_src_file_2 = os.path.join('/mock/tmp', 'input.csv', 'part-00001.csv')
        expected_dst_file_2 = os.path.join('/mock/output', 'input.csv')

        # Assert shutil.copy2 was called correctly
        mock_copy2.assert_any_call(expected_src_file_1, expected_dst_file_1)
        mock_copy2.assert_any_call(expected_src_file_2, expected_dst_file_2)

    @patch('os.environ', {'TMP_DIR': '/mock/tmp', 'OUTPUT_DIR': '/mock/output'})
    @patch('os.listdir')
    @patch('shutil.copy2')
    def test_no_matching_files(self, mock_copy2, mock_listdir):
        # Mock the list of files in the source directory
        mock_listdir.return_value = ['some_other_file.csv', 'another_file.txt']

        # Call the function
        refactor_csv_file('input.csv')

        # Assert shutil.copy2 was not called since there are no matching files
        mock_copy2.assert_not_called()

    @patch("builtins.open", new_callable=mock_open, read_data="xml_path: /path/to/xml")
    @patch("yaml.full_load", return_value={"xml_path": "/path/to/xml"})
    def test_get_app_config(self, mock_yaml_load, mock_open_file):
        expected_config = "/path/to/xml"

        # Call the function
        app_config = get_app_config('config.yaml')

        # Assertions
        mock_open_file.assert_called_once_with('config.yaml', 'r')
        mock_yaml_load.assert_called_once()
        self.assertEqual(app_config, expected_config)

    @patch("builtins.open", new_callable=mock_open, read_data="invalid_yaml")
    @patch("yaml.full_load", return_value={})
    def test_get_app_config_invalid_yaml(self, mock_yaml_load, mock_open_file):
        expected_config = None

        # Call the function
        app_config = get_app_config('config.yaml')

        # Assertions
        mock_open_file.assert_called_once_with('config.yaml', 'r')
        mock_yaml_load.assert_called_once()
        self.assertEqual(app_config, expected_config)

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local").appName("test").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.input_schema = StructType([
            StructField("MCC", StringType(), True),
            StructField("MNC", StringType(), True),
            StructField("Country_code", ArrayType(StringType()), True),
            StructField("NDC", ArrayType(StringType()), True),
            StructField("SN_Start", ArrayType(StringType()), True),
            StructField("SN_Stop", ArrayType(StringType()), True)
        ])

        self.output_schema = StructType([
            StructField("MCC", StringType(), True),
            StructField("MNC", StringType(), True),
            StructField("Country_code", StringType(), True),
            StructField("NDC", StringType(), True),
            StructField("SN_Start", StringType(), True),
            StructField("SN_Stop", StringType(), True)
        ])

        self.data = [
            ("001", "01", ["03", "04"], ["123", "124"], ["0000", "1000"], ["9999", "1999"]),
            ("001", "02", ["05", "06"], ["122", "126"], ["0100", "12"], ["32", "55"])
        ]

        self.df = self.spark.createDataFrame(self.data, self.input_schema)
        self.config = {
            "MCC": "MCC",
            "MNC": "MNC",
            "Country_code": "Country_code",
            "NDC": "NDC",
            "SN_Start": "SN_Start",
            "SN_Stop": "SN_Stop",
            "target_MCC": "001",
            "target_MNC": "01"
        }

    def test_transform_df(self):
        result_df = transform_df(self.df, self.config)

        expected_data = [
            ("001", "01", "03", "123", "0000", "9999"),
            ("001", "01", "04", "124", "1000", "1999")
        ]

        expected_df = self.spark.createDataFrame(expected_data, self.output_schema)

        result_data = [tuple(row) for row in result_df.collect()]
        expected_data = [tuple(row) for row in expected_df.collect()]

        self.assertEqual(result_data, expected_data)