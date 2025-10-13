"""This module defines the version information for the wkmigrate library.

Management of version identifiers for releases uses the `bumpversion` python package which
supports automatic modification of files when the version labels need to be modified.

See: https://pypi.org/project/bumpversion/

Note the use of `get_version` for method name to conform with bumpversion conventions.
"""

from collections import namedtuple
import re
import logging


VersionInfo = namedtuple("VersionInfo", ["major", "minor", "patch", "release", "build"])


def get_version(version: str) -> VersionInfo:
    """Gets the version info object for wkmigrate.
    :parameter version: Version to get info for as a ``str``; must be compatible with the `bump` package
    :return: Version info as a ``namedtuple``
    """
    version_expression = re.compile(
        r"(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+){0,1}(?P<release>\D*)(?P<build>\d*)"
    )
    match = version_expression.match(version)
    if match is None:
        raise ValueError(f"Invalid version format: {version}")
    major, minor, patch, release, build = match.groups()
    version_info = VersionInfo(major, minor, patch, release, build)
    logger = logging.getLogger(__name__)
    logger.info(f"Version : {version_info}")
    return version_info


__version__ = "0.0.0"  # NOTE: MANAGED BY BUMPVERSION; DO NOT EDIT DIRECTLY
__version_info__ = get_version(__version__)
