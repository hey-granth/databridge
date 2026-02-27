"""
Tests for the DataBridge pipeline API.
"""

import io
import json

from django.test import TestCase
from rest_framework.test import APIClient

from pipelines.models import OutputData, Pipeline, PipelineRun


class PipelineAPITest(TestCase):
    def setUp(self):
        self.client = APIClient()

    def test_successful_pipeline_run_csv(self):
        """End-to-end: create pipeline, upload CSV, verify completed run with CSV output."""
        resp = self.client.post(
            "/api/pipelines/",
            data=json.dumps(
                {
                    "name": "Filter Pipeline",
                    "configuration": {
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

        csv_content = b"id,name,value\n1,Alice,50\n2,Bob,200\n3,Charlie,300\n"
        f = io.BytesIO(csv_content)
        f.name = "input.csv"

        resp = self.client.post(
            f"/api/pipelines/{pipeline_id}/run/",
            data={"file": f, "destination": "csv"},
            format="multipart",
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["status"], "completed")
        self.assertIsNone(data["error_message"])

        # output_file is committed via save=True — verify filename prefix pattern
        # (Django may append a suffix if the file already exists, e.g. pipeline_1_run_1_abc123.csv)
        self.assertIsNotNone(data["output_file"])
        run_id = data["id"]
        expected_prefix = f"pipeline_{pipeline_id}_run_{run_id}"
        self.assertIn(expected_prefix, data["output_file"])

        # Verify output_file is persisted — fetch the run fresh from DB via the API
        resp = self.client.get(f"/api/runs/{run_id}/")
        self.assertEqual(resp.status_code, 200)
        fetched = resp.json()
        self.assertEqual(fetched["status"], "completed")
        self.assertIsNotNone(fetched["output_file"])
        self.assertIn(expected_prefix, fetched["output_file"])

    def test_successful_pipeline_run_database(self):
        """End-to-end: create pipeline, upload CSV, verify completed run with DB output."""
        pipeline = Pipeline.objects.create(
            name="DB Pipeline",
            configuration={
                "filters": [{"column": "age", "operator": "gt", "value": 18}],
            },
        )

        csv_content = b"id,name,age\n1,Alice,25\n2,Bob,15\n3,Charlie,30\n"
        f = io.BytesIO(csv_content)
        f.name = "input.csv"

        resp = self.client.post(
            f"/api/pipelines/{pipeline.pk}/run/",
            data={"file": f, "destination": "database"},
            format="multipart",
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["status"], "completed")
        self.assertIsNone(data["output_file"])   # no CSV file for DB destination
        self.assertIsNone(data["error_message"])

        # Verify rows were written to OutputData
        run = PipelineRun.objects.get(pk=data["id"])
        self.assertEqual(run.output_rows.count(), 2)  # only Alice (25) and Charlie (30)

    def test_invalid_configuration_rejected(self):
        """Creating a pipeline with an invalid transform config returns 400."""
        resp = self.client.post(
            "/api/pipelines/",
            data=json.dumps(
                {
                    "name": "Bad Config",
                    "configuration": {
                        "filters": "not-a-list",  # must be a list
                    },
                }
            ),
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 400)
        error = resp.json()["error"]
        self.assertEqual(error["code"], "VALIDATION_ERROR")

    def test_invalid_destination_rejected(self):
        """Run endpoint rejects unsupported destination values with a structured error."""
        pipeline = Pipeline.objects.create(
            name="Dest Test Pipeline",
            configuration={},
        )

        f = io.BytesIO(b"id,name\n1,Alice\n")
        f.name = "input.csv"

        resp = self.client.post(
            f"/api/pipelines/{pipeline.pk}/run/",
            data={"file": f, "destination": "s3"},
            format="multipart",
        )
        self.assertEqual(resp.status_code, 400)
        error = resp.json()["error"]
        self.assertEqual(error["code"], "VALIDATION_ERROR")
        self.assertIn("destination", error["details"])

    def test_missing_column_failure(self):
        """A run fails when config references a column not in the data."""
        pipeline = Pipeline.objects.create(
            name="Bad Column Pipeline",
            configuration={
                "column_selection": ["id", "nonexistent_column"],
            },
        )

        f = io.BytesIO(b"id,name,value\n1,Alice,100\n")
        f.name = "input.csv"

        resp = self.client.post(
            f"/api/pipelines/{pipeline.pk}/run/",
            data={"file": f, "destination": "csv"},
            format="multipart",
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["status"], "failed")
        self.assertIn("nonexistent_column", data["error_message"])

        # Verify error_message is persisted — update_fields=["status", "error_message"]
        resp = self.client.get(f"/api/runs/{data['id']}/")
        self.assertEqual(resp.json()["status"], "failed")
        self.assertIn("nonexistent_column", resp.json()["error_message"])

    def test_unsupported_file_type_rejected(self):
        """Run endpoint rejects files with unsupported extensions before creating a run."""
        pipeline = Pipeline.objects.create(
            name="File Type Pipeline",
            configuration={},
        )

        f = io.BytesIO(b"some data")
        f.name = "data.txt"

        resp = self.client.post(
            f"/api/pipelines/{pipeline.pk}/run/",
            data={"file": f, "destination": "csv"},
            format="multipart",
        )
        self.assertEqual(resp.status_code, 400)
        error = resp.json()["error"]
        self.assertEqual(error["code"], "UNSUPPORTED_FILE_TYPE")
        # No run should have been created
        self.assertEqual(PipelineRun.objects.filter(pipeline=pipeline).count(), 0)
