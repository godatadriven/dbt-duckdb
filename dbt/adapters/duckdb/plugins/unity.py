from __future__ import annotations

from enum import Enum
from typing import Any
from typing import Dict

import pyarrow as pa
from pyarrow_unity.model import model_unity_schema
from pyarrow_unity.model import UCSupportedTypeLiteral
from unitycatalog import Unitycatalog
from unitycatalog.types import GenerateTemporaryTableCredentialResponse
from unitycatalog.types.table_create_params import Column

from . import BasePlugin
from ..utils import find_secrets_by_type
from ..utils import SourceConfig
from ..utils import TargetConfig


class StorageFormat(str, Enum):
    """Enum class for the storage formats supported by the plugin."""

    DELTA = "DELTA"


def uc_schema_exists(client: Unitycatalog, schema_name: str, catalog_name: str = "unity") -> bool:
    """Check if a UC schema exists in the catalog."""
    schema_list_request = client.schemas.list(catalog_name=catalog_name)

    if not schema_list_request.schemas:
        return False

    return schema_name in [schema.name for schema in schema_list_request.schemas]


def uc_table_exists(
    client: Unitycatalog, table_name: str, schema_name: str, catalog_name: str = "unity"
) -> bool:
    """Check if a UC table exists in the catalog."""

    table_list_request = client.tables.list(catalog_name=catalog_name, schema_name=schema_name)

    if not table_list_request.tables:
        return False

    return table_name in [table.name for table in table_list_request.tables]


def uc_get_storage_credentials(
    client: Unitycatalog, catalog_name: str, schema_name: str, table_name: str
) -> dict:
    """Get temporary table credentials for a UC table if they exist."""

    # Get the table ID

    if not uc_table_exists(client, table_name, schema_name, catalog_name):
        return {}

    table_response = client.tables.retrieve(full_name=f"{catalog_name}.{schema_name}.{table_name}")

    if not table_response.table_id:
        return {}

    # Get the temporary table credentials
    creds: GenerateTemporaryTableCredentialResponse = client.temporary_table_credentials.create(
        operation="READ_WRITE", table_id=table_response.table_id
    )

    if creds.aws_temp_credentials:
        return {
            "AWS_ACCESS_KEY_ID": creds.aws_temp_credentials.access_key_id,
            "AWS_SECRET_ACCESS_KEY": creds.aws_temp_credentials.secret_access_key,
            "AWS_SESSION_TOKEN": creds.aws_temp_credentials.session_token,
        }

    return {}


def create_table_if_not_exists(
    uc_client: Unitycatalog,
    table_name: str,
    schema_name: str,
    catalog_name: str,
    storage_location: str,
    schema: list[Column],
    storage_format: UCSupportedTypeLiteral,
):
    """Create or update a Unitycatalog table."""

    if not uc_schema_exists(uc_client, schema_name, catalog_name):
        uc_client.schemas.create(catalog_name=catalog_name, name=schema_name)

    if not uc_table_exists(uc_client, table_name, schema_name, catalog_name):
        uc_client.tables.create(
            catalog_name=catalog_name,
            columns=schema,
            data_source_format=storage_format,
            name=table_name,
            schema_name=schema_name,
            table_type="EXTERNAL",
            storage_location=storage_location,
        )
    else:
        # TODO: Add support for schema checks/schema evolution with existing schema and dataframe schema
        pass


class Plugin(BasePlugin):
    # The name of the catalog
    catalog_name: str = "unity"

    # The default storage format
    default_format = StorageFormat.DELTA

    # The Unitycatalog client
    uc_client: Unitycatalog

    # The AWS region
    aws_region: str

    def initialize(self, config: Dict[str, Any]):
        # Assert that the credentials and secrets are present
        assert self.creds is not None, "Credentials are required for the plugin!"
        assert self.creds.secrets is not None, "Secrets are required for the plugin!"

        # Find the UC secret
        uc_secret = find_secrets_by_type(self.creds.secrets, "UC")

        # Get AWS region from the UC secret
        self.aws_region = uc_secret["aws_region"]

        # Get the endpoint from the UC secret
        host_and_port = uc_secret["endpoint"]

        # Get the token from the UC secret
        token = uc_secret["token"]

        # Get the optional base path from the plugin config
        api_base_path = config.get("api_base_path", "api/2.1/unity-catalog")

        # Construct the full base URL
        catalog_base_url = f"{host_and_port}/{api_base_path}"

        # Prism mocks the UC server to http://127.0.0.1:4010 with no option to specify a basePath (api/2.1/unity-catalog)
        # https://github.com/stoplightio/prism/discussions/906
        # This is why we need to check if we are running in pytest and only use the host_and_port
        # Otherwise we will not be able to connect to the mock UC server
        self.uc_client: Unitycatalog = Unitycatalog(
            base_url=catalog_base_url, default_headers={"Authorization": f"Bearer {token}"}
        )

    def load(self, source_config: SourceConfig):
        raise NotImplementedError("Loading data to Unitycatalog is not supported!")

    def store(self, target_config: TargetConfig, df: pa.lib.Table = None):
        # Assert that the target_config has a location and relation identifier
        assert target_config.location is not None, "Location is required for storing data!"
        assert (
            target_config.relation.identifier is not None
        ), "Relation identifier is required to name the table!"

        # Get required variables from the target configuration
        table_path = target_config.location.path
        table_name = target_config.relation.identifier

        # Get optional variables from the target configuration
        mode = target_config.config.get("mode", "overwrite")
        schema_name = target_config.config.get("schema")

        # If schema is not provided or empty set to default"
        if not schema_name or schema_name == "":
            schema_name = "default"

        storage_options = target_config.config.get("storage_options", {})
        partition_key = target_config.config.get("partition_key", None)
        unique_key = target_config.config.get("unique_key", None)

        # Get the storage format from the plugin configuration
        storage_format = self.plugin_config.get("format", self.default_format)

        # Convert the pa schema to columns
        converted_schema = model_unity_schema(schema=df.schema)

        # Create the table in the Unitycatalog if it does not exist
        create_table_if_not_exists(
            uc_client=self.uc_client,
            table_name=table_name,
            schema_name=schema_name,
            catalog_name=self.catalog_name,
            storage_location=table_path,
            schema=converted_schema,
            storage_format=storage_format,
        )

        # extend the storage options with the aws region
        storage_options["AWS_REGION"] = self.aws_region

        # extend the storage options with the temporary table credentials
        storage_options = storage_options | uc_get_storage_credentials(
            self.uc_client, self.catalog_name, schema_name, table_name
        )

        if storage_format == StorageFormat.DELTA:
            from .delta import delta_write

            delta_write(
                mode=mode,
                table_path=table_path,
                df=df,
                storage_options=storage_options,
                partition_key=partition_key,
                unique_key=unique_key,
            )
        else:
            raise NotImplementedError(f"Writing storage format {storage_format} not supported!")
