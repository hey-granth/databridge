"""
Pipeline service — orchestrates reading, transforming, and writing.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from django.core.files.base import ContentFile
from django.core.files.uploadedfile import UploadedFile

from pipelines.engine.transforms import (
    ColumnMismatchError,
    InvalidExpressionError,
    TransformError,
    run_transforms,
)
from pipelines.models import OutputData, Pipeline, PipelineRun

ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".xls"}


class PipelineExecutionError(Exception):
    """Raised for pre-execution validation failures (file type, etc.)."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(message)


def _read_file(uploaded_file: UploadedFile) -> pd.DataFrame:
    """Read an uploaded CSV or Excel file into a DataFrame."""
    ext = Path(uploaded_file.name).suffix.lower()
    if ext == ".csv":
        return pd.read_csv(uploaded_file)
    elif ext in (".xlsx", ".xls"):
        return pd.read_excel(uploaded_file, engine="openpyxl")
    raise PipelineExecutionError(
        code="UNSUPPORTED_FILE_TYPE",
        message=f"File type '{ext}' is not supported. Allowed: .csv, .xlsx, .xls",
    )


def _write_csv(df: pd.DataFrame) -> bytes:
    """Serialize a DataFrame to CSV bytes."""
    return df.to_csv(index=False).encode("utf-8")


def _write_database(df: pd.DataFrame, run: PipelineRun) -> None:
    """Write DataFrame rows to the OutputData table."""
    rows = df.to_dict(orient="records")
    OutputData.objects.bulk_create(
        [OutputData(pipeline_run=run, data=row) for row in rows]
    )


def run_pipeline(
    pipeline: Pipeline, uploaded_file: UploadedFile, destination: str = "csv"
) -> PipelineRun:
    """
    Execute a pipeline: validate file → create run → read → transform → write.

    Args:
        pipeline:      The Pipeline instance to execute.
        uploaded_file: The uploaded CSV or Excel file.
        destination:   Where to write output — "csv" or "database".

    Returns the completed or failed PipelineRun instance.
    Raises PipelineExecutionError for pre-run validation failures.
    """
    config = pipeline.configuration

    # Validate file extension
    ext = Path(uploaded_file.name).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise PipelineExecutionError(
            code="UNSUPPORTED_FILE_TYPE",
            message=f"File type '{ext}' is not supported. Allowed: {sorted(ALLOWED_EXTENSIONS)}",
        )

    # Create run record with uploaded file
    run = PipelineRun.objects.create(
        pipeline=pipeline,
        input_file=uploaded_file,
        status=PipelineRun.Status.PENDING,
    )

    try:
        # Read (seek to start — file pointer may have advanced after save)
        uploaded_file.seek(0)
        df = _read_file(uploaded_file)

        # Transform
        df = run_transforms(df, config)

        # Write
        if destination == "csv":
            filename = f"pipeline_{pipeline.pk}_run_{run.pk}.csv"
            csv_bytes = _write_csv(df)
            # save=True writes the file to storage AND calls run.save() immediately,
            # so output_file is committed in its own UPDATE before status is set.
            run.output_file.save(filename, ContentFile(csv_bytes), save=True)

        elif destination == "database":
            _write_database(df, run)

        run.status = PipelineRun.Status.COMPLETED
        run.save(update_fields=["status"])

    except (ColumnMismatchError, InvalidExpressionError, TransformError) as exc:
        run.status = PipelineRun.Status.FAILED
        run.error_message = str(exc)
        run.save(update_fields=["status", "error_message"])

    except Exception as exc:
        run.status = PipelineRun.Status.FAILED
        run.error_message = str(exc)
        run.save(update_fields=["status", "error_message"])

    return run
