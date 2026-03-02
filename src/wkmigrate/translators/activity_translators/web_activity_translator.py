"""This module defines a translator for translating Web activities.

Translators in this module normalize ADF Web activity payloads into internal
representations. Each translator must validate required fields, parse the URL,
HTTP method, optional body, and optional headers, and emit ``UnsupportedValue``
objects for any unparsable inputs.
"""

from wkmigrate.models.ir.pipeline import WebActivity
from wkmigrate.models.ir.unsupported import UnsupportedValue


def translate_web_activity(activity: dict, base_kwargs: dict) -> WebActivity | UnsupportedValue:
    """
    Translates an ADF Web activity into a ``WebActivity`` object.

    Args:
        activity: Web activity definition as a ``dict``.
        base_kwargs: Common activity metadata.

    Returns:
        ``WebActivity`` representation of the HTTP request task.
    """
    url = activity.get("url")
    if not isinstance(url, str) or not url:
        return UnsupportedValue(activity, "Missing value 'url' for Web activity")

    method = activity.get("method")
    if not isinstance(method, str) or not method:
        return UnsupportedValue(activity, "Missing value 'method' for Web activity")

    return WebActivity(
        **base_kwargs,
        url=url,
        method=method.upper(),
        body=activity.get("body"),
        headers=activity.get("headers"),
    )
