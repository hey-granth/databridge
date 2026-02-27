# databridge

A configuration-driven ETL pipeline API built with Django and Django REST Framework.

---

## Problem Statement

The assessment requires building a backend service that accepts structured file uploads, applies a set of user-defined transformations to the data, and writes the result to a configurable destination.

Configuration-driven processing means the transformation logic is not hardcoded. Instead, each pipeline stores a JSON configuration object that describes what operations to apply — column renames, filters, computed fields, and so on — without requiring code changes. The same codebase handles any combination of supported transformations by reading from this config at runtime.

---

## Approach and Design Decisions

### Architectural Layering

The project is structured into four distinct layers, each with a single responsibility:

- **API layer** (`pipelines/api/`) — DRF ViewSets and serializers handle HTTP concerns: request parsing, input validation, response serialization, and error formatting. Views delegate immediately to the service layer and contain no business logic.

- **Service layer** (`pipelines/services/pipeline_service.py`) — Orchestrates the pipeline execution lifecycle: validates the uploaded file, creates the `PipelineRun` record, calls the transformation engine, writes output, and updates run status. This is the only place that coordinates across layers.

- **Transformation engine** (`pipelines/engine/transforms.py`) — Stateless functions that each accept a pandas DataFrame and return a modified DataFrame. The engine applies them in a fixed, predictable order. Keeping all transform logic in one module avoids scattered business logic and makes the processing pipeline easy to follow.

- **Config validation** (`pipelines/services/config_validator.py`) — Validates the structure of the transformation config when a pipeline is created, not at run time. This surfaces configuration errors early and prevents invalid pipelines from being saved.

### Configuration on the Pipeline Model

The `configuration` JSON field is stored on `Pipeline` rather than passed at request time because a pipeline represents a reusable processing definition. The same pipeline can be triggered multiple times against different files. Storing config on the model separates the "what to do" (pipeline definition) from the "what to do it to" (uploaded file per run).

### Synchronous Execution

Transformations run synchronously within the request-response cycle. For the scope of this assessment — moderate-size files and single-user usage — this is appropriate. It avoids the operational complexity of a task queue while keeping the execution flow transparent and debuggable.

### Validation Strategy

Validation is split into two phases:

- **Config validation** occurs at pipeline creation time. The `config_validator` checks structural correctness — required keys, valid operators, parseable expressions. This prevents a malformed config from being saved.
- **Runtime validation** occurs during execution. Missing columns, unreadable files, and unsupported file extensions are caught by the service layer and result in a `FAILED` run with a descriptive `error_message`.

---

## Architecture Overview

```
POST /api/pipelines/{id}/run/
        |
        v
PipelineRunTriggerSerializer       — validates file + destination
        |
        v
PipelineViewSet.run()              — extracts validated data, calls service
        |
        v
pipeline_service.run_pipeline()    — validates file type, creates PipelineRun
        |
        v
_read_file()                       — reads CSV or Excel into a DataFrame
        |
        v
run_transforms()                   — applies transforms from config in order
        |
        v
_write_csv() / _write_database()   — writes output to file or OutputData table
        |
        v
PipelineRun.save()                 — commits final status and output path
        |
        v
PipelineRunSerializer              — serializes run record
        |
        v
Response (200)
```

---

## Supported Features

### File Types

| Extension | Format |
|-----------|--------|
| `.csv` | Comma-separated values |
| `.xlsx` | Excel (Open XML) |
| `.xls` | Excel (legacy binary) |

Source type is inferred from the file extension — it does not need to be specified in the config.

### Transformations

Applied in the following fixed order:

| Transform | Config Key | Description |
|-----------|------------|-------------|
| Column rename | `column_mapping` | Rename columns using `{"old": "new"}` mapping |
| Column selection | `column_selection` | Keep only the listed columns |
| Row filtering | `filters` | Filter rows using `eq`, `gt`, `lt`, or `contains` |
| Computed fields | `computed_fields` | Add new columns using `concat(...)` or `add(a, b)` |
| Drop columns | `drop_columns` | Remove listed columns from output |

### Output Destinations

| Value | Behaviour |
|-------|-----------|
| `csv` | Writes output to a file in `media/outputs/`. File is downloadable via API. |
| `database` | Inserts each row as a JSON record into the `OutputData` table. |

Destination is passed as a parameter when triggering a run, not stored in the pipeline config.

---

## API Endpoints

### Create a Pipeline

```
POST /api/pipelines/
Content-Type: application/json
```

**Request body:**
```json
{
  "name": "Sales Filter",
  "configuration": {
    "filters": [
      {"column": "revenue", "operator": "gt", "value": 1000}
    ],
    "drop_columns": ["internal_id"]
  }
}
```

**Response (201):**
```json
{
  "id": 1,
  "name": "Sales Filter",
  "configuration": {
    "filters": [{"column": "revenue", "operator": "gt", "value": 1000}],
    "drop_columns": ["internal_id"]
  },
  "created_at": "2026-02-28T12:00:00Z"
}
```

---

### Trigger a Pipeline Run

```
POST /api/pipelines/{id}/run/
Content-Type: multipart/form-data
```

**Form fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `file` | File | Yes | CSV or Excel file to process |
| `destination` | String | No | `csv` (default) or `database` |

**Response (200):**
```json
{
  "id": 1,
  "pipeline": 1,
  "input_file": "/media/uploads/data.csv",
  "output_file": "/media/outputs/pipeline_1_run_1.csv",
  "status": "completed",
  "error_message": null,
  "created_at": "2026-02-28T12:00:05Z"
}
```

**Failed run response (200):**
```json
{
  "id": 2,
  "pipeline": 1,
  "input_file": "/media/uploads/data.csv",
  "output_file": null,
  "status": "failed",
  "error_message": "column_selection references missing columns: ['nonexistent_column']",
  "created_at": "2026-02-28T12:01:00Z"
}
```

---

### Get Run Status

```
GET /api/runs/{id}/
```

Returns the `PipelineRun` record including status, file paths, and any error message.

---

### Download Output File

```
GET /api/runs/{id}/download/
```

Returns the output CSV as a file download. Returns 404 if the run has no output file (e.g. database destination or failed run).

---

### Error Response Format

All error responses follow this structure:

```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid request parameters.",
    "details": {
      "destination": ["\"s3\" is not a valid choice."]
    }
  }
}
```

| Code | Cause |
|------|-------|
| `VALIDATION_ERROR` | Invalid request payload or pipeline config |
| `UNSUPPORTED_FILE_TYPE` | Uploaded file has an unsupported extension |
| `HTTP_404` | Resource not found |

---

## Configuration Format

The `configuration` field on a Pipeline accepts a JSON object with any combination of the following keys. All keys are optional.

```json
{
  "column_mapping": {
    "emp_id": "employee_id",
    "dept": "department"
  },
  "column_selection": ["employee_id", "department", "salary"],
  "filters": [
    {"column": "salary", "operator": "gt", "value": 50000},
    {"column": "department", "operator": "contains", "value": "Eng"}
  ],
  "computed_fields": [
    {"name": "annual_salary", "expression": "add(salary, bonus)"},
    {"name": "label", "expression": "concat(department, ' - ', employee_id)"}
  ],
  "drop_columns": ["bonus", "internal_notes"]
}
```

### Field Reference

| Key | Type | Description |
|-----|------|-------------|
| `column_mapping` | Object | `{"source_name": "target_name"}` — applied before all other transforms |
| `column_selection` | Array of strings | Columns to keep; all others are discarded |
| `filters` | Array of filter objects | Each filter requires `column`, `operator`, and `value` |
| `computed_fields` | Array of field objects | Each requires `name` and `expression` |
| `drop_columns` | Array of strings | Columns to remove after all other transforms |

### Filter Operators

| Operator | Behaviour |
|----------|-----------|
| `eq` | Exact match |
| `gt` | Greater than |
| `lt` | Less than |
| `contains` | Substring match (string columns) |

### Expression Functions

| Function | Signature | Description |
|----------|-----------|-------------|
| `concat` | `concat(col1, 'literal', col2, ...)` | Concatenates column values and string literals |
| `add` | `add(col1, col2)` | Adds two numeric column values |

Nested function calls are not supported. Arguments are either column names or single-quoted string literals.

### Validation Behaviour

Config is validated at pipeline creation time. Errors are returned as structured JSON with field-level detail. A pipeline with an invalid config is not saved. Column existence is validated at run time against the actual uploaded data.

---

## Setup Instructions

```bash
# Install dependencies
pip install -r requirements.txt

# Apply database migrations
python manage.py migrate

# Start the development server
python manage.py runserver
```

The server runs at `http://127.0.0.1:8000` by default.

---

## Manual Testing Guide

### Using Postman

**1. Create a pipeline**

- Method: `POST`
- URL: `http://127.0.0.1:8000/api/pipelines/`
- Body: raw, JSON
```json
{
  "name": "Test Pipeline",
  "configuration": {
    "filters": [{"column": "age", "operator": "gt", "value": 18}]
  }
}
```
- Note the `id` in the response.

**2. Trigger a run**

- Method: `POST`
- URL: `http://127.0.0.1:8000/api/pipelines/1/run/`
- Body: form-data
  - Key: `file`, Type: File — select a `.csv` or `.xlsx` file
  - Key: `destination`, Type: Text — value: `csv` or `database`
- Note the `id` from the response.

**3. Check run status**

- Method: `GET`
- URL: `http://127.0.0.1:8000/api/runs/1/`

**4. Download output**

- Method: `GET`
- URL: `http://127.0.0.1:8000/api/runs/1/download/`
- Click "Send and Download" to save the file.

### Common Error Scenarios

| Scenario | How to reproduce | Expected response |
|----------|-----------------|-------------------|
| Invalid config | Set `filters` to a string instead of a list | 400 with `VALIDATION_ERROR` |
| Unsupported file type | Upload a `.txt` file | 400 with `UNSUPPORTED_FILE_TYPE` |
| Invalid destination | Set `destination` to `"s3"` | 400 with `VALIDATION_ERROR` |
| Missing column | Config references a column not in the file | Run status `failed` with descriptive `error_message` |
| No file uploaded | Submit run request with no file field | 400 with `VALIDATION_ERROR` |

---

## Running Tests

```bash
python manage.py test pipelines
```

Six tests cover the following scenarios:

1. Successful run with CSV output — verifies status, output file path, and persistence
2. Successful run with database output — verifies `OutputData` row count
3. Invalid configuration rejected at pipeline creation
4. Invalid destination value rejected at run time
5. Missing column causes run failure with persisted error message
6. Unsupported file extension rejected before a run record is created

---

## Assumptions and Limitations

- **Synchronous execution** — each run blocks the request until completion. Not suitable for large files in production.
- **File size** — bounded by Django's default upload limits and available server memory, since the full file is loaded into a DataFrame.
- **SQLite** — used by default. Suitable for development and single-user assessment use.
- **Single output table** — all database-destination rows are written to the `OutputData` model as JSON. There is no schema inference or dynamic table creation.
- **No authentication** — all endpoints are open. Not suitable for multi-user or production deployment without adding an auth layer.
- **No nested expressions** — computed field expressions support a single function call with column references and string literals only.

---

## Future Improvements

- **Asynchronous execution** — offload pipeline runs to a task queue (e.g. Celery) to avoid blocking requests on large files.
- **Chunked processing** — stream large files in chunks rather than loading the full DataFrame into memory.
- **Authentication and authorisation** — add token-based auth and per-user pipeline ownership.
- **Storage abstraction** — support configurable output destinations (e.g. S3, GCS) rather than local filesystem only.
