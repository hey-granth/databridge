# DataBridge

A configuration-driven ETL pipeline API built with Django and Django REST Framework. Upload CSV or Excel files, apply JSON-configured transformations, and write results to a CSV file or database table.

## Architecture

```
Request → DRF View → Pipeline Service → Transforms → Write Output → Update Run Status
```

- **`pipelines/api/`** — DRF views, serializers, URL routing, error handling
- **`pipelines/engine/transforms.py`** — All transformation logic in one module
- **`pipelines/services/pipeline_service.py`** — Orchestrates read → transform → write
- **`pipelines/models.py`** — Pipeline, PipelineRun, OutputData models

## Models

| Model | Purpose |
|-------|---------|
| **Pipeline** | Stores name + JSON configuration |
| **PipelineRun** | Tracks each execution (status, input/output files, errors) |
| **OutputData** | Stores transformed rows when destination is `database` |

## Configuration Format

The `configuration` JSON field on a Pipeline supports these keys:

```json
{
  "source_type": "csv",
  "destination_type": "csv",
  "destination_filename": "output.csv",
  "column_mapping": {"old_name": "new_name"},
  "column_selection": ["col1", "col2"],
  "filters": [
    {"column": "age", "operator": "gt", "value": 18}
  ],
  "computed_fields": [
    {"name": "full_name", "expression": "concat(first, ' ', last)"}
  ],
  "drop_columns": ["temp_col"]
}
```

### Supported Transformations

| Transform | Description |
|-----------|-------------|
| `column_mapping` | Rename columns: `{old: new}` |
| `column_selection` | Keep only listed columns |
| `filters` | Filter rows. Operators: `eq`, `gt`, `lt`, `contains` |
| `computed_fields` | Add columns. Functions: `concat(...)`, `add(a, b)` |
| `drop_columns` | Remove listed columns |

### Destination Types

- `"csv"` — Writes output to a downloadable CSV file. Requires `destination_filename`.
- `"database"` — Writes rows to the `OutputData` table as JSON.

### Source Types

- `"csv"` — Accepts `.csv` files
- `"excel"` — Accepts `.xlsx` / `.xls` files

## Setup

```bash
# Clone and enter project
cd databridge

# Install dependencies
pip install -r requirements.txt

# Run migrations
python manage.py migrate

# Start server
python manage.py runserver
```

## API Endpoints

### Create a Pipeline

```
POST /api/pipelines/
Content-Type: application/json

{
  "name": "My Pipeline",
  "configuration": {
    "source_type": "csv",
    "destination_type": "csv",
    "destination_filename": "result.csv",
    "filters": [{"column": "value", "operator": "gt", "value": 100}]
  }
}
```

**Response (201):**
```json
{
  "id": 1,
  "name": "My Pipeline",
  "configuration": { ... },
  "created_at": "2026-02-28T12:00:00Z"
}
```

### Trigger a Pipeline Run

```
POST /api/pipelines/1/run/
Content-Type: multipart/form-data

file=@data.csv
```

**Response (200):**
```json
{
  "id": 1,
  "pipeline": 1,
  "input_file": "uploads/data.csv",
  "output_file": "outputs/result.csv",
  "status": "completed",
  "error_message": null,
  "created_at": "2026-02-28T12:00:05Z"
}
```

### Get Run Status

```
GET /api/runs/1/
```

### Download Output File

```
GET /api/runs/1/download/
```

Returns the CSV file as a download (if the run produced one).

## Running Tests

```bash
python manage.py test pipelines
```

Three tests cover:
1. **Successful pipeline run** — end-to-end CSV upload, transform, and output
2. **Invalid configuration** — rejected with 400 on unsupported source type
3. **Missing column** — run fails with descriptive error when config references absent column

## Assumptions and Limitations

- **Synchronous execution only** — pipeline runs block until complete.
- **SQLite** — default database; suitable for development and assessment.
- **Single table for DB output** — all database-destination rows go to `OutputData` as JSON.
- **No authentication** — endpoints are open (appropriate for assessment scope).
- **File size** — limited by Django's default upload settings.
- **No nested expressions** — computed fields support flat function calls only.

