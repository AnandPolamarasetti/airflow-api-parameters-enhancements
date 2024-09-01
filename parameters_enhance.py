from __future__ import annotations

import logging
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, Container, Dict, Optional, TypeVar, Union

import pendulum
from sqlalchemy import text
from sqlalchemy.sql import Select

from airflow.api_connexion.exceptions import BadRequest
from airflow.configuration import conf
from airflow.utils import timezone

if TYPE_CHECKING:
    from datetime import datetime

log = logging.getLogger(__name__)

def validate_is_timezone(value: datetime) -> None:
    """Validate that a datetime is not naive."""
    if value.tzinfo is None:
        raise BadRequest("Invalid datetime format", detail="Naive datetime is disallowed")

def format_datetime(value: str) -> datetime:
    """
    Format datetime strings to datetime objects.

    Datetime format parser for args since connexion doesn't parse datetimes.
    Raises BadRequest on failure.

    :param value: Datetime string in ISO 8601 format or with 'Z' suffix
    :return: Parsed datetime object
    """
    value = value.strip()
    if not value.endswith("Z"):
        value = value.replace(" ", "+")
    try:
        return timezone.parse(value)
    except (pendulum.parsing.ParserError, TypeError) as err:
        raise BadRequest("Incorrect datetime argument", detail=str(err))

def check_limit(value: int) -> int:
    """
    Check if the limit does not exceed the configured maximum value.

    :param value: Requested limit
    :return: Validated limit
    """
    max_val = conf.getint("api", "maximum_page_limit")  # User-configured max page limit
    fallback = conf.getint("api", "fallback_page_limit")

    if value > max_val:
        log.warning(
            "The limit param value %d exceeds the configured maximum page limit %d",
            value,
            max_val,
        )
        return max_val
    if value <= 0:
        if value == 0:
            return fallback
        raise BadRequest("Page limit must be a positive integer")
    return value

T = TypeVar("T", bound=Callable)

def format_parameters(params_formatters: Dict[str, Callable[[Any], Any]]) -> Callable[[T], T]:
    """
    Create a decorator to convert parameters using provided formatters.

    :param params_formatters: Dictionary mapping parameter names to formatter functions
    :return: Decorated function with formatted parameters
    """
    def format_parameters_decorator(func: T) -> T:
        @wraps(func)
        def wrapped_function(*args, **kwargs):
            for key, formatter in params_formatters.items():
                if key in kwargs:
                    kwargs[key] = formatter(kwargs[key])
            return func(*args, **kwargs)

        return cast(T, wrapped_function)

    return format_parameters_decorator

def apply_sorting(
    query: Select,
    order_by: str,
    to_replace: Optional[Dict[str, str]] = None,
    allowed_attrs: Optional[Container[str]] = None,
) -> Select:
    """Apply sorting to the query based on the order_by parameter."""
    lstriped_orderby = order_by.lstrip("-")
    if allowed_attrs and lstriped_orderby not in allowed_attrs:
        raise BadRequest(
            detail=f"Ordering with '{lstriped_orderby}' is disallowed or "
            f"the attribute does not exist on the model"
        )
    if to_replace:
        lstriped_orderby = to_replace.get(lstriped_orderby, lstriped_orderby)
    if order_by.startswith("-"):
        order_by = f"{lstriped_orderby} desc"
    else:
        order_by = f"{lstriped_orderby} asc"
    return query.order_by(text(order_by))
