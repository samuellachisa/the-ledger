"""
Input validation for MCP tool arguments.

Validates arguments against the tool's inputSchema before dispatch.
Returns a structured error instead of leaking Python stack traces.

Uses jsonschema if available, falls back to manual required-field check.
"""
from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)

try:
    import jsonschema
    _HAS_JSONSCHEMA = True
except ImportError:
    _HAS_JSONSCHEMA = False
    logger.debug("jsonschema not installed — using basic required-field validation")


class ValidationError(Exception):
    def __init__(self, message: str, field: Optional[str] = None):
        self.field = field
        super().__init__(message)


def validate_arguments(tool_name: str, arguments: dict, schema: dict) -> None:
    """
    Validate tool arguments against the tool's inputSchema.

    Raises ValidationError with a user-friendly message if validation fails.
    Never raises on unknown tools (let the dispatcher handle that).
    """
    if not schema:
        return

    if _HAS_JSONSCHEMA:
        try:
            jsonschema.validate(instance=arguments, schema=schema)
        except jsonschema.ValidationError as e:
            field = ".".join(str(p) for p in e.absolute_path) or e.path.peek() if e.path else None
            raise ValidationError(
                f"Invalid argument for tool '{tool_name}': {e.message}",
                field=str(field) if field else None,
            ) from e
    else:
        # Fallback: check required fields only
        required = schema.get("required", [])
        for field in required:
            if field not in arguments:
                raise ValidationError(
                    f"Missing required argument '{field}' for tool '{tool_name}'",
                    field=field,
                )

        # Basic type checks for numeric fields
        props = schema.get("properties", {})
        for key, value in arguments.items():
            prop_schema = props.get(key, {})
            expected_type = prop_schema.get("type")
            if expected_type == "number" and not isinstance(value, (int, float)):
                raise ValidationError(
                    f"Argument '{key}' must be a number for tool '{tool_name}'",
                    field=key,
                )
            if expected_type == "integer" and not isinstance(value, int):
                raise ValidationError(
                    f"Argument '{key}' must be an integer for tool '{tool_name}'",
                    field=key,
                )
            minimum = prop_schema.get("minimum")
            maximum = prop_schema.get("maximum")
            if minimum is not None and isinstance(value, (int, float)) and value < minimum:
                raise ValidationError(
                    f"Argument '{key}' must be >= {minimum} for tool '{tool_name}'",
                    field=key,
                )
            if maximum is not None and isinstance(value, (int, float)) and value > maximum:
                raise ValidationError(
                    f"Argument '{key}' must be <= {maximum} for tool '{tool_name}'",
                    field=key,
                )
