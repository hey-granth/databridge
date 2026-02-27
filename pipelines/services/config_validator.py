"""
Pipeline configuration validator.

Validates the JSON configuration stored on a Pipeline model instance.
Returns a list of error dicts with 'field' and 'message' keys.
"""

from __future__ import annotations

import re
from typing import Any

SUPPORTED_SOURCE_TYPES = {"csv", "excel"}
SUPPORTED_DESTINATION_TYPES = {"database", "csv"}
SUPPORTED_FILTER_OPERATORS = {"eq", "gt", "lt", "contains"}
SUPPORTED_EXPRESSION_FUNCS = {"concat", "add"}

_EXPR_RE = re.compile(r"^(\w+)\((.+)\)$", re.DOTALL)


def validate_config(config: Any) -> list[dict[str, str]]:
    """Validate a pipeline config dict. Returns a list of errors (empty = valid)."""
    errors: list[dict[str, str]] = []

    if not isinstance(config, dict):
        return [{"field": "configuration", "message": "Must be a JSON object."}]

    # source_type
    source_type = config.get("source_type")
    if source_type is None:
        errors.append({"field": "configuration.source_type", "message": "Required."})
    elif source_type not in SUPPORTED_SOURCE_TYPES:
        errors.append(
            {
                "field": "configuration.source_type",
                "message": f"Unsupported. Allowed: {sorted(SUPPORTED_SOURCE_TYPES)}.",
            }
        )

    # destination_type
    dest_type = config.get("destination_type")
    if dest_type is None:
        errors.append(
            {"field": "configuration.destination_type", "message": "Required."}
        )
    elif dest_type not in SUPPORTED_DESTINATION_TYPES:
        errors.append(
            {
                "field": "configuration.destination_type",
                "message": f"Unsupported. Allowed: {sorted(SUPPORTED_DESTINATION_TYPES)}.",
            }
        )

    # destination-specific fields
    if dest_type == "csv" and not config.get("destination_filename"):
        errors.append(
            {
                "field": "configuration.destination_filename",
                "message": "Required when destination_type is 'csv'.",
            }
        )

    # column_mapping
    mapping = config.get("column_mapping")
    if mapping is not None and not isinstance(mapping, dict):
        errors.append(
            {"field": "configuration.column_mapping", "message": "Must be an object."}
        )

    # column_selection
    selection = config.get("column_selection")
    if selection is not None and (
        not isinstance(selection, list) or len(selection) == 0
    ):
        errors.append(
            {
                "field": "configuration.column_selection",
                "message": "Must be a non-empty list.",
            }
        )

    # filters
    filters = config.get("filters")
    if filters is not None:
        if not isinstance(filters, list):
            errors.append(
                {"field": "configuration.filters", "message": "Must be a list."}
            )
        else:
            for i, f in enumerate(filters):
                prefix = f"configuration.filters[{i}]"
                if not isinstance(f, dict):
                    errors.append({"field": prefix, "message": "Must be an object."})
                    continue
                for key in ("column", "operator", "value"):
                    if key not in f:
                        errors.append(
                            {"field": f"{prefix}.{key}", "message": "Required."}
                        )
                if "operator" in f and f["operator"] not in SUPPORTED_FILTER_OPERATORS:
                    errors.append(
                        {
                            "field": f"{prefix}.operator",
                            "message": f"Unsupported. Allowed: {sorted(SUPPORTED_FILTER_OPERATORS)}.",
                        }
                    )

    # computed_fields
    computed = config.get("computed_fields")
    if computed is not None:
        if not isinstance(computed, list):
            errors.append(
                {"field": "configuration.computed_fields", "message": "Must be a list."}
            )
        else:
            for i, cf in enumerate(computed):
                prefix = f"configuration.computed_fields[{i}]"
                if not isinstance(cf, dict):
                    errors.append({"field": prefix, "message": "Must be an object."})
                    continue
                if "name" not in cf:
                    errors.append({"field": f"{prefix}.name", "message": "Required."})
                if "expression" not in cf:
                    errors.append(
                        {"field": f"{prefix}.expression", "message": "Required."}
                    )
                else:
                    m = _EXPR_RE.match(cf["expression"].strip())
                    if not m:
                        errors.append(
                            {
                                "field": f"{prefix}.expression",
                                "message": f"Cannot parse: '{cf['expression']}'.",
                            }
                        )
                    elif m.group(1) not in SUPPORTED_EXPRESSION_FUNCS:
                        errors.append(
                            {
                                "field": f"{prefix}.expression",
                                "message": f"Unsupported function: '{m.group(1)}'.",
                            }
                        )

    # drop_columns
    drop = config.get("drop_columns")
    if drop is not None and not isinstance(drop, list):
        errors.append(
            {"field": "configuration.drop_columns", "message": "Must be a list."}
        )

    return errors
