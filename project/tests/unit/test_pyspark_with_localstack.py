from project.tests.unit.config_local_integration_test_pyspark import PySparkLocalIntegrationTestCase


class IntegrationTestUDFS3Handler(PySparkLocalIntegrationTestCase):
    """
    Local Integration Test class for S3 handler
    """

    def test_df_count_test_1(self):
        """
            Basic test to verify the count of a CSV uploaded on the fake S3
        """
        path = f"s3a://{self.bucket_test_name}/cars.csv"
        df = self.spark.read.csv(path)

        self.assertEqual(df.count(), 408, "It should have 408 rows")
