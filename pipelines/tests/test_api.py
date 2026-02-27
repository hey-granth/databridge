"""
Tests for the DataBridge pipeline API.

Three required tests:
1. Successful pipeline run
2. Invalid configuration rejected
3. Missing column failure
"""

import io
import json

from django.test import TestCase
from rest_framework.test import APIClient

from pipelines.models import Pipeline


class PipelineAPITest(TestCase):
    def setUp(self):
        self.client = APIClient()

    def test_successful_pipeline_run(self):
        """End-to-end: create pipeline, upload CSV, verify completed run."""
        # Create pipeline with a filter
        resp = self.client.post(
            "/api/pipelines/",
            data=json.dumps(
                {
                    "name": "Filter Pipeline",
                    "configuration": {
                        "source_type": "csv",
                        "destination_type": "csv",
                        "destination_filename": "result.csv",
                        "filters": [
                            {"column": "value", "operator": "gt", "value": 100}
                        ],
                    },
                }
            ),
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 201)
        pipeline_id = resp.json()["id"]

        # Upload CSV and trigger run
        csv_content = b"id,name,value\n1,Alice,50\n2,Bob,200\n3,Charlie,300\n"
        f = io.BytesIO(csv_content)
        f.name = "input.csv"

        resp = self.client.post(
            f"/api/pipelines/{pipeline_id}/run/",
            data={"file": f},
            format="multipart",
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["status"], "completed")
        self.assertIsNotNone(data["output_file"])
        self.assertIsNone(data["error_message"])

        # Verify run can be retrieved
        run_id = data["id"]
        resp = self.client.get(f"/api/runs/{run_id}/")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["status"], "completed")

    def test_invalid_configuration_rejected(self):
        """Creating a pipeline with invalid config returns 400."""
        resp = self.client.post(
            "/api/pipelines/",
            data=json.dumps(
                {
                    "name": "Bad Config",
                    "configuration": {
                        "source_type": "parquet",  # unsupported
                        "destination_type": "database",
                    },
                }
            ),
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 400)

    def test_missing_column_failure(self):
        """A run fails when config references a column not in the data."""
        pipeline = Pipeline.objects.create(
            name="Bad Column Pipeline",
            configuration={
                "source_type": "csv",
                "destination_type": "csv",
                "destination_filename": "out.csv",
                "column_selection": ["id", "nonexistent_column"],
            },
        )

        csv_content = b"id,name,value\n1,Alice,100\n"
        f = io.BytesIO(csv_content)
        f.name = "input.csv"

        resp = self.client.post(
            f"/api/pipelines/{pipeline.pk}/run/",
            data={"file": f},
            format="multipart",
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["status"], "failed")
        self.assertIn("nonexistent_column", data["error_message"])
