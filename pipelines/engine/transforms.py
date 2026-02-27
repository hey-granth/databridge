"""
Transformation functions for pipeline data processing.

Each function operates on a pandas DataFrame and returns a new DataFrame.
"""

from __future__ import annotations

import re
from typing import Any

import pandas as pd


class TransformError(Exception):
    """Base error for transformation failures."""


class ColumnMismatchError(TransformError):
    """A config element references a column not present in the DataFrame."""


class InvalidExpressionError(TransformError):
    """A computed-field expression could not be parsed or evaluated."""


def apply_column_mapping(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    """Rename columns according to mapping {old_name: new_name}."""
    missing = set(mapping.keys()) - set(df.columns)
    if missing:
        raise ColumnMismatchError(
            f"column_mapping references columns not in data: {sorted(missing)}"
        )
    return df.rename(columns=mapping)


def apply_column_selection(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Keep only the listed columns, in the order given."""
    missing = set(columns) - set(df.columns)
    if missing:
        raise ColumnMismatchError(
            f"column_selection references missing columns: {sorted(missing)}"
        )
    return df[columns]


FILTER_OPERATORS = {
    "eq": lambda s, v: s == v,
    "gt": lambda s, v: s > v,
    "lt": lambda s, v: s < v,
    "contains": lambda s, v: s.astype(str).str.contains(str(v), na=False),
}


def apply_filters(df: pd.DataFrame, filters: list[dict[str, Any]]) -> pd.DataFrame:
    """Apply row filters (ANDed together)."""
    for f in filters:
        col, op, value = f["column"], f["operator"], f["value"]
        if col not in df.columns:
            raise ColumnMismatchError(f"Filter references missing column: '{col}'")
        if op not in FILTER_OPERATORS:
            raise TransformError(f"Unsupported filter operator: '{op}'")
        mask = FILTER_OPERATORS[op](df[col], value)
        df = df.loc[mask].reset_index(drop=True)
    return df


# --- Computed fields ---

_EXPR_RE = re.compile(r"^(\w+)\((.+)\)$", re.DOTALL)
_LITERAL_RE = re.compile(r"^'(.*)'$")

EXPRESSION_FUNCTIONS = {
    "concat": lambda vals: "".join(str(v) for v in vals),
    "add": lambda vals: vals[0] + vals[1],
}


def _parse_args(raw: str) -> list[str]:
    """Split comma-separated args, respecting single-quoted literals."""
    args, current, in_quote = [], [], False
    for char in raw:
        if char == "'":
            in_quote = not in_quote
            current.append(char)
        elif char == "," and not in_quote:
            args.append("".join(current).strip())
            current = []
        else:
            current.append(char)
    if current:
        args.append("".join(current).strip())
    return args


def _resolve_arg(arg: str, row: pd.Series) -> Any:
    """Resolve an argument — literal string or column reference."""
    m = _LITERAL_RE.match(arg)
    return m.group(1) if m else row[arg]


def _eval_expression(expr: str, row: pd.Series) -> Any:
    """Evaluate a computed-field expression against a row."""
    m = _EXPR_RE.match(expr.strip())
    if not m:
        raise InvalidExpressionError(f"Cannot parse expression: '{expr}'")

    func_name, raw_args = m.group(1), m.group(2)
    if func_name not in EXPRESSION_FUNCTIONS:
        raise InvalidExpressionError(f"Unsupported function: '{func_name}'")

    args = _parse_args(raw_args)
    resolved = [_resolve_arg(a, row) for a in args]
    return EXPRESSION_FUNCTIONS[func_name](resolved)


def apply_computed_fields(
    df: pd.DataFrame, computed_fields: list[dict[str, Any]]
) -> pd.DataFrame:
    """Add new columns based on computed field definitions."""
    for field in computed_fields:
        name, expression = field["name"], field["expression"]

        # Validate column references exist
        m = _EXPR_RE.match(expression.strip())
        if m:
            for arg in _parse_args(m.group(2)):
                if not _LITERAL_RE.match(arg) and arg not in df.columns:
                    raise ColumnMismatchError(
                        f"Computed field '{name}' references missing column: '{arg}'"
                    )

        df[name] = df.apply(lambda row: _eval_expression(expression, row), axis=1)
    return df


def apply_drop_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Drop the listed columns from the DataFrame."""
    present = [c for c in columns if c in df.columns]
    return df.drop(columns=present)


def run_transforms(df: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    """
    Apply all configured transformations in order:
    1. column_mapping  2. column_selection  3. filters
    4. computed_fields  5. drop_columns
    """
    if "column_mapping" in config:
        df = apply_column_mapping(df, config["column_mapping"])
    if "column_selection" in config:
        df = apply_column_selection(df, config["column_selection"])
    if "filters" in config:
        df = apply_filters(df, config["filters"])
    if "computed_fields" in config:
        df = apply_computed_fields(df, config["computed_fields"])
    if "drop_columns" in config:
        df = apply_drop_columns(df, config["drop_columns"])
    return df
