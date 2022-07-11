"""
Module to use for pyspark integration tests
It will integrate a mock for AWS Services & a local PySpark
"""
import unittest
import os
from pathlib import Path
import pytest
from pyspark.sql import SparkSession
import boto3

from project.tests.context import ROOT_PROJECT_PATH

ENDPOINT_URL = os.getenv("S3_ENDPOINT", "http://127.0.0.1:4566")
AWS_REGION = 'eu-west-1'
BUCKET_TEST_NAME = 'test-dev-databricks-bucket'


class PySparkLocalIntegrationTestCase(unittest.TestCase):
    """
    Class to inherit for pyspark local integration test
    """
    bucket_test_name = BUCKET_TEST_NAME

    @pytest.fixture(autouse=True)
    def setup_bucket(self, request):
        """
            For each test function, surround it with the 2 followings methods: _upload_files() & _clean_bucket()

            :param request: the fixture request
        """
        self._upload_files(request.node.name)
        yield
        self._clean_bucket()

    @classmethod
    def setUpClass(cls):
        """
        Set up a SparkSession as a local config that will be reuse for all pyspark tests
        Set up a mock boto3
        """
        cls.spark = SparkSession \
            .builder \
            .master("local") \
            .config("spark.jars.packages",
                    'org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901') \
            .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                    'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
            .config('spark.hadoop.fs.s3a.access.key', 'dummy') \
            .config('spark.hadoop.fs.s3a.secret.key', 'dummy') \
            .config('spark.hadoop.fs.s3a.impl', "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config('spark.hadoop.fs.s3a.path.style.access', "true") \
            .config('spark.hadoop.fs.s3a.endpoint', ENDPOINT_URL) \
            .appName("test-pyspark-session") \
            .getOrCreate()

        cls.sc = cls.spark.sparkContext
        cls.s3_client = boto3.client("s3", endpoint_url=ENDPOINT_URL, aws_access_key_id='dummy',
                                     aws_secret_access_key='dummy', use_ssl=False)

        cls.s3_client.create_bucket(
            Bucket=cls.bucket_test_name,
            CreateBucketConfiguration={
                'LocationConstraint': AWS_REGION
            },
            ObjectOwnership='BucketOwnerPreferred'
        )

    @classmethod
    def tearDownClass(cls):
        """
        Tear down the SparkSession
        """
        cls.sc.stop()

        cls.s3_client.delete_bucket(
            Bucket=cls.bucket_test_name
        )

    def _upload_files(self, func_name):
        """
        Upload files corresponding to the method being tested.
        """
        root_dir = Path(f"{ROOT_PROJECT_PATH}/tests/unit/resources/s3/{func_name}")
        file_list = [f for f in root_dir.glob('**/*') if f.is_file()]
        for file in file_list:
            self.s3_client.upload_file(str(file),
                                       self.bucket_test_name,
                                       str(file.relative_to(root_dir))
                                       )

    def _clean_bucket(self):
        """
        Remove every objects from the test bucket
        """
        response = self.s3_client.list_objects_v2(Bucket=self.bucket_test_name)

        for obj in response['Contents']:
            self.s3_client.delete_object(Bucket=self.bucket_test_name, Key=obj['Key'])
