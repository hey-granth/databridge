"""
Custom DRF exception handler.

Wraps all error responses in a consistent JSON format:

    {"error": {"code": "...", "message": "...", "details": ...}}
"""

from rest_framework.exceptions import ValidationError
from rest_framework.response import Response
from rest_framework.views import exception_handler


def custom_exception_handler(exc, context) -> Response | None:
    response = exception_handler(exc, context)

    if response is None:
        return None

    # Already formatted by the view
    if isinstance(response.data, dict) and "error" in response.data:
        return response

    # DRF ValidationError
    if isinstance(exc, ValidationError):
        response.data = {
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "Request payload failed validation.",
                "details": response.data,
            }
        }
        return response

    # Other DRF exceptions (404, 405, etc.)
    detail = (
        response.data.get("detail", "An error occurred.")
        if isinstance(response.data, dict)
        else str(response.data)
    )
    response.data = {
        "error": {
            "code": f"HTTP_{response.status_code}",
            "message": str(detail),
        }
    }
    return response
