"""Command-line interface for wkmigrate."""

from __future__ import annotations

import json
import sys

import click

from wkmigrate.clients.factory_client import FactoryClient
from wkmigrate.models.ir.profile import FactoryProfile
from wkmigrate.profiler import format_profile, profile_factory


@click.group()
def cli() -> None:
    """wkmigrate -- Azure Data Factory migration toolkit."""


@cli.command()
@click.option("--tenant-id", required=True, envvar="AZURE_TENANT_ID", help="Azure AD tenant ID.")
@click.option("--client-id", required=True, envvar="AZURE_CLIENT_ID", help="Service-principal client ID.")
@click.option("--client-secret", required=True, envvar="AZURE_CLIENT_SECRET", help="Service-principal client secret.")
@click.option("--subscription-id", required=True, envvar="AZURE_SUBSCRIPTION_ID", help="Azure subscription ID.")
@click.option("--resource-group", required=True, envvar="AZURE_RESOURCE_GROUP", help="Resource group name.")
@click.option("--factory-name", required=True, envvar="ADF_FACTORY_NAME", help="Data Factory name.")
@click.option(
    "--output",
    "output_format",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text",
    help="Output format (default: text).",
)
def profile(
    tenant_id: str,
    client_id: str,
    client_secret: str,
    subscription_id: str,
    resource_group: str,
    factory_name: str,
    output_format: str,
) -> None:
    """Profile an Azure Data Factory to assess migration readiness."""
    client = FactoryClient(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
        subscription_id=subscription_id,
        resource_group_name=resource_group,
        factory_name=factory_name,
    )

    try:
        result: FactoryProfile = profile_factory(client)
    except Exception as exc:
        click.echo(f"Error: {exc}", err=True)
        raise SystemExit(1) from exc

    if output_format == "json":
        _print_json(result)
    else:
        click.echo(format_profile(result))


def _print_json(profile_result: FactoryProfile) -> None:
    """Serialize a FactoryProfile to JSON and write to stdout."""
    from dataclasses import asdict

    json.dump(asdict(profile_result), sys.stdout, indent=2)
    sys.stdout.write("\n")
