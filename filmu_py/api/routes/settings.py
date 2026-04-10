"""Settings compatibility routes aligned with frontend BFF expectations."""

from __future__ import annotations

import json
import logging
from copy import deepcopy
from typing import Annotated, Any, cast

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query, Request, status
from pydantic import ValidationError

from filmu_py.api.deps import get_db, require_permissions
from filmu_py.api.deps import get_settings as dep_get_settings
from filmu_py.api.models import MessageResponse
from filmu_py.audit import audit_action
from filmu_py.config import Settings, set_runtime_settings
from filmu_py.db.runtime import DatabaseRuntime
from filmu_py.services.settings_service import save_settings as persist_settings_blob

router = APIRouter(prefix="/settings", tags=["settings"])
logger = logging.getLogger(__name__)


def _settings_dump(settings_obj: Settings) -> dict[str, Any]:
    """Return the exact compatibility projection of runtime settings."""

    return settings_obj.to_compatibility_dict()


def _require_object_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Ensure a settings write payload is a JSON object."""

    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Payload must be object"
        )
    return payload


def _validate_compatibility_payload(payload: dict[str, Any]) -> Settings:
    """Validate one full compatibility payload into the typed runtime settings model."""

    try:
        return Settings.from_compatibility_dict(payload)
    except ValidationError as exc:
        serialized_errors: list[dict[str, Any]] = []
        for error in exc.errors():
            normalized = dict(error)
            ctx = normalized.get("ctx")
            if isinstance(ctx, dict):
                normalized["ctx"] = {key: str(value) for key, value in ctx.items()}
            serialized_errors.append(normalized)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=serialized_errors,
        ) from exc


async def _persist_runtime_settings(
    *,
    request: Request,
    db: DatabaseRuntime,
    payload: dict[str, Any],
) -> Settings:
    """Validate, persist, and activate one full compatibility settings payload."""

    validated = _validate_compatibility_payload(_require_object_payload(payload))
    await persist_settings_blob(db, payload)
    request.app.state.resources.settings = validated
    request.app.state.resources.plugin_settings_payload = json.loads(json.dumps(payload))
    set_runtime_settings(validated)
    return validated


def _path_get(root: dict[str, Any], path: str) -> Any:
    """Resolve dot-path from nested settings, returning None when missing."""

    if not path:
        return None

    current: Any = root
    for segment in path.split("."):
        if not isinstance(current, dict) or segment not in current:
            return None
        current = current[segment]
    return current


def _path_set(root: dict[str, Any], path: str, value: Any) -> bool:
    """Set a value on an existing dot-path; returns False when path is invalid."""

    if not path:
        return False

    *parents, leaf = path.split(".")
    current: object = root
    for segment in parents:
        if not isinstance(current, dict) or segment not in current:
            return False
        current = cast(dict[str, Any], current)[segment]

    if not isinstance(current, dict) or leaf not in current:
        return False

    cast(dict[str, Any], current)[leaf] = value
    return True


def _split_csv(value: str) -> list[str]:
    """Split and normalize comma-separated values."""

    return [part.strip() for part in value.split(",") if part.strip()]


def _json_type_for_value(value: Any) -> Any:
    """Infer JSON schema type descriptor from Python value."""

    if value is None:
        return ["null", "string"]
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    return "string"


def _schema_for_value(value: Any) -> dict[str, Any]:
    """Build a compatibility JSON-schema-like node for one runtime value."""

    if isinstance(value, dict):
        return {
            "type": "object",
            "properties": {key: _schema_for_value(child) for key, child in value.items()},
        }

    if isinstance(value, list):
        if not value:
            return {"type": "array", "items": {"type": "string"}}

        sample = next((entry for entry in value if entry is not None), None)
        if isinstance(sample, dict):
            return {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {key: _schema_for_value(child) for key, child in sample.items()},
                },
            }
        return {"type": "array", "items": {"type": _json_type_for_value(sample)}}

    return {"type": _json_type_for_value(value)}


def _is_top_level_section(settings_data: dict[str, Any], path: str, value: Any) -> bool:
    """Return whether one requested path is a top-level nested settings section."""

    return path in settings_data and isinstance(value, dict)


def _extract_section_update(
    *,
    settings_data: dict[str, Any],
    path: str,
    values: dict[str, Any],
) -> tuple[dict[str, Any] | None, bool]:
    """Return one merged section payload for flat or wrapped compatibility writes."""

    current_value = settings_data.get(path)
    if not isinstance(current_value, dict):
        return None, False

    if path in values and isinstance(values[path], dict):
        merged = deepcopy(current_value)
        merged.update(cast(dict[str, Any], values[path]))
        return merged, True

    matched = {key: values[key] for key in current_value if key in values}
    if not matched:
        return None, False

    merged = deepcopy(current_value)
    merged.update(matched)
    return merged, True


@router.get("", operation_id="settings.get_current", response_model=dict[str, Any])
async def get_current_settings(
    settings: Annotated[Settings, Depends(dep_get_settings)],
) -> dict[str, Any]:
    """Return the full compatibility settings payload from the active runtime instance."""

    return _settings_dump(settings)


@router.put(
    "",
    operation_id="settings.put_current",
    response_model=dict[str, Any],
    dependencies=[Depends(require_permissions("settings:write"))],
)
async def put_current_settings(
    request: Request,
    db: Annotated[DatabaseRuntime, Depends(get_db)],
    payload: Annotated[dict[str, Any], Body(...)],
) -> dict[str, Any]:
    """Validate, persist, and activate a full compatibility settings payload."""

    validated = await _persist_runtime_settings(request=request, db=db, payload=payload)
    audit_action(
        request,
        action="settings.put_current",
        target="runtime.settings",
        details={"top_level_keys": sorted(payload)},
    )
    return validated.to_compatibility_dict()


@router.get("/schema", operation_id="settings.get_schema", response_model=dict[str, Any])
async def get_settings_schema(
    settings: Annotated[Settings, Depends(dep_get_settings)],
) -> dict[str, Any]:
    """Return a JSON-schema-like shape for currently available runtime settings fields."""

    settings_data = _settings_dump(settings)
    properties = {key: _schema_for_value(value) for key, value in settings_data.items()}

    return {
        "title": "Settings",
        "type": "object",
        "properties": properties,
        "required": ["api_key"],
    }


@router.get(
    "/schema/keys", operation_id="settings.get_schema_for_keys", response_model=dict[str, Any]
)
async def get_settings_schema_for_keys(
    settings: Annotated[Settings, Depends(dep_get_settings)],
    keys: str = Query(..., min_length=1),
    title: str = Query("Settings"),
) -> dict[str, Any]:
    """Return a filtered schema for requested keys.

    Unknown keys are ignored to preserve frontend tab compatibility during phased rollout.
    """

    settings_data = _settings_dump(settings)
    full_schema = await get_settings_schema(settings)
    properties = cast(dict[str, Any], full_schema["properties"])
    requested = _split_csv(keys)
    filtered: dict[str, Any] = {}
    required: list[str] = []
    for key in requested:
        if key not in properties:
            continue
        value = settings_data.get(key)
        property_schema = properties[key]
        if _is_top_level_section(settings_data, key, value):
            nested_properties = cast(dict[str, Any], property_schema.get("properties", {}))
            filtered.update(nested_properties)
            continue
        filtered[key] = property_schema
        if key in cast(list[str], full_schema["required"]):
            required.append(key)

    return {
        "title": title,
        "type": "object",
        "properties": filtered,
        "required": required,
    }


@router.get("/load", operation_id="settings.load", response_model=MessageResponse)
async def load_settings() -> MessageResponse:
    """Compatibility no-op load endpoint."""

    return MessageResponse(message="Settings loaded!")


@router.post("/save", operation_id="settings.save", response_model=MessageResponse)
async def save_settings() -> MessageResponse:
    """Compatibility no-op save endpoint."""

    return MessageResponse(message="Settings saved!")


@router.get("/get/all", operation_id="settings.get_all", response_model=dict[str, Any])
async def get_all_settings(
    settings: Annotated[Settings, Depends(dep_get_settings)],
) -> dict[str, Any]:
    """Return all runtime settings as a JSON object."""

    return await get_current_settings(settings)


@router.get("/get/{paths}", operation_id="settings.get", response_model=dict[str, Any])
async def get_settings_for_paths(
    settings: Annotated[Settings, Depends(dep_get_settings)],
    paths: str = Path(..., min_length=1),
) -> dict[str, Any]:
    """Return values for comma-separated dot-paths.

    Missing paths return `null` values to avoid breaking frontend tab hydration.
    """

    settings_data = _settings_dump(settings)
    result: dict[str, Any] = {}
    for path in _split_csv(paths):
        value = _path_get(settings_data, path)
        if _is_top_level_section(settings_data, path, value):
            result.update(cast(dict[str, Any], value))
            continue
        result[path] = value
    return result


@router.post(
    "/set/all",
    operation_id="settings.set_all",
    response_model=MessageResponse,
    dependencies=[Depends(require_permissions("settings:write"))],
)
async def set_all_settings(
    request: Request,
    db: Annotated[DatabaseRuntime, Depends(get_db)],
    payload: Annotated[dict[str, Any], Body(...)],
) -> MessageResponse:
    """Compatibility alias for persisting a full settings payload over POST."""

    await _persist_runtime_settings(request=request, db=db, payload=payload)
    audit_action(
        request,
        action="settings.set_all",
        target="runtime.settings",
        details={"top_level_keys": sorted(payload)},
    )
    return MessageResponse(message="All settings updated successfully!")


@router.post(
    "/set/{paths}",
    operation_id="settings.set",
    response_model=MessageResponse,
    dependencies=[Depends(require_permissions("settings:write"))],
)
async def set_settings_for_paths(
    request: Request,
    settings: Annotated[Settings, Depends(dep_get_settings)],
    db: Annotated[DatabaseRuntime, Depends(get_db)],
    values: Annotated[dict[str, Any], Body(...)],
    paths: str = Path(..., min_length=1),
) -> MessageResponse:
    """Compatibility endpoint for path-based updates.

    Current phase validates path/value alignment and path existence; persistence is deferred.
    """

    try:
        requested_paths = _split_csv(paths)
        values = _require_object_payload(values)
        settings_data = _settings_dump(settings)
        missing: list[str] = []
        prepared_updates: dict[str, Any] = {}
        for path in requested_paths:
            current_value = _path_get(settings_data, path)
            if _is_top_level_section(settings_data, path, current_value):
                section_update, found = _extract_section_update(
                    settings_data=settings_data,
                    path=path,
                    values=values,
                )
                if not found:
                    missing.append(path)
                    continue
                prepared_updates[path] = section_update
                continue
            if path not in values:
                missing.append(path)
                continue
            prepared_updates[path] = values[path]

        if missing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Missing values for paths: {', '.join(missing)}",
            )

        invalid_paths = [
            path
            for path in requested_paths
            if not _path_set(settings_data, path, prepared_updates[path])
        ]
        if invalid_paths:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid paths: {', '.join(invalid_paths)}",
            )

        await _persist_runtime_settings(request=request, db=db, payload=settings_data)
        audit_action(
            request,
            action="settings.set_paths",
            target="runtime.settings",
            details={"paths": requested_paths},
        )
        return MessageResponse(message="Settings updated successfully.")
    except Exception as e:
        detail_getter = getattr(e, "errors", None)
        detail: Any
        if callable(detail_getter):
            try:
                detail = detail_getter()
            except Exception:
                detail = str(e)
        elif isinstance(e, HTTPException):
            detail = e.detail
        else:
            detail = str(e)
        logger.error(
            "settings_set_failed",
            extra={"keys": paths, "error": str(e), "detail": detail},
            exc_info=True,
        )
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) from e
