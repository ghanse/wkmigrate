"""Translator for SFTP linked service definitions.

This module normalizes ADF SFTP linked-service payloads into
``SftpLinkedService`` objects.
"""

from uuid import uuid4

from wkmigrate.models.ir.linked_services import SftpLinkedService
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.utils import get_value_or_unsupported

_DEFAULT_SFTP_PORT = 22


def translate_sftp_spec(sftp_spec: dict) -> SftpLinkedService | UnsupportedValue:
    """
    Parses an SFTP linked service definition into an ``SftpLinkedService`` object.

    Args:
        sftp_spec: Linked-service definition from Azure Data Factory.

    Returns:
        SFTP linked-service metadata as an ``SftpLinkedService`` object.
    """
    if not sftp_spec:
        return UnsupportedValue(value=sftp_spec, message="Missing SFTP linked service definition")

    properties = sftp_spec.get("properties", {})
    host = get_value_or_unsupported(properties, "host", "SFTP linked service properties")
    if isinstance(host, UnsupportedValue):
        return UnsupportedValue(value=sftp_spec, message=host.message)

    port = properties.get("port", _DEFAULT_SFTP_PORT)

    return SftpLinkedService(
        service_name=sftp_spec.get("name", str(uuid4())),
        service_type="sftp",
        host=host,
        port=port,
        user_name=properties.get("user_name"),
        authentication_type=properties.get("authentication_type"),
    )
