from __future__ import annotations

import json
from enum import Enum
from typing import Any
from typing import Dict
from typing import Literal

import pyarrow as pa
from deltalake import DeltaTable
from unitycatalog import Unitycatalog
from unitycatalog.types import TableInfo
from unitycatalog.types.table_create_params import Column

from . import BasePlugin
from ..utils import find_secrets_by_type
from ..utils import SourceConfig
from ..utils import TargetConfig


class StorageFormat(str, Enum):
    """Enum class for the storage formats supported by the plugin."""

    DELTA = "DELTA"


class StorageLocationMissingError(Exception):
    """Exception raised when the storage location is missing for a unity catalog table."""


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


def get_storage_location(
    client: Unitycatalog, table_name: str, schema_name: str, catalog_name: str = "unity"
) -> str:
    """Get the storage location of a UC table."""
    table: TableInfo = client.tables.retrieve(
        full_name=f"{catalog_name}.{schema_name}.{table_name}"
    )

    if table.storage_location is None:
        raise StorageLocationMissingError(
            f"Table {catalog_name}.{schema_name}.{table_name} does not have a storage location!"
        )
    return table.storage_location


UCSupportedTypeLiteral = Literal[
    "BOOLEAN",
    "BYTE",
    "SHORT",
    "INT",
    "LONG",
    "FLOAT",
    "DOUBLE",
    "DATE",
    "TIMESTAMP",
    "TIMESTAMP_NTZ",
    "STRING",
    "BINARY",
    "DECIMAL",
    "INTERVAL",
    "ARRAY",
    "STRUCT",
    "MAP",
    "CHAR",
    "NULL",
    "USER_DEFINED_TYPE",
    "TABLE_TYPE",
]


def pyarrow_type_to_supported_uc_json_type(data_type: pa.DataType) -> UCSupportedTypeLiteral:
    """Convert a PyArrow data type to a supported Unitycatalog JSON type."""
    if pa.types.is_boolean(data_type):
        return "BOOLEAN"
    elif pa.types.is_int8(data_type):
        return "BYTE"
    elif pa.types.is_int16(data_type):
        return "SHORT"
    elif pa.types.is_int32(data_type):
        return "INT"
    elif pa.types.is_int64(data_type):
        return "LONG"
    elif pa.types.is_float32(data_type):
        return "FLOAT"
    elif pa.types.is_float64(data_type):
        return "DOUBLE"
    elif pa.types.is_date32(data_type):
        return "DATE"
    elif pa.types.is_timestamp(data_type):
        return "TIMESTAMP"
    elif pa.types.is_string(data_type):
        return "STRING"
    elif pa.types.is_binary(data_type):
        return "BINARY"
    elif pa.types.is_decimal(data_type):
        return "DECIMAL"
    elif pa.types.is_duration(data_type):
        return "INTERVAL"
    elif pa.types.is_list(data_type):
        return "ARRAY"
    elif pa.types.is_struct(data_type):
        return "STRUCT"
    elif pa.types.is_map(data_type):
        return "MAP"
    elif pa.types.is_null(data_type):
        return "NULL"
    else:
        raise NotImplementedError(f"Type {data_type} not supported")


def pyarrow_schema_to_columns(schema: pa.Schema) -> list[Column]:
    """Convert a PyArrow schema to a list of Unitycatalog Column objects."""
    columns = []

    for i, field in enumerate(schema):
        data_type = field.type
        json_type = pyarrow_type_to_supported_uc_json_type(data_type)

        column = Column(
            name=field.name,
            type_name=json_type,
            nullable=field.nullable,
            comment=f"Field {field.name}",  # Generic comment, modify as needed
            position=i,
            type_json=json.dumps(
                {
                    "name": field.name,
                    "type": json_type,
                    "nullable": field.nullable,
                    "metadata": field.metadata or {},
                }
            ),
            type_precision=0,
            type_scale=0,
            type_text=json_type,
        )

        # Adjust type precision and scale for decimal types
        if pa.types.is_decimal(data_type):
            column["type_precision"] = data_type.precision
            column["type_scale"] = data_type.scale

        columns.append(column)

    return columns


class Plugin(BasePlugin):
    # The name of the catalog
    catalog_name: str = "unity"

    # The default storage format
    default_format = StorageFormat.DELTA

    # The Unitycatalog client
    uc_client: Unitycatalog

    def initialize(self, config: Dict[str, Any]):
        # Assert that the credentials and secrets are present
        assert self.creds is not None, "Credentials are required for the plugin!"
        assert self.creds.secrets is not None, "Secrets are required for the plugin!"

        # Find the UC secret
        uc_secret = find_secrets_by_type(self.creds.secrets, "UC")

        # Get the endpoint from the UC secret
        catalog_base_url = uc_secret["endpoint"]

        # Initialize the Unitycatalog client
        self.uc_client: Unitycatalog = Unitycatalog(
            base_url=f"{catalog_base_url}/api/2.1/unity-catalog"
        )

    def load(self, source_config: SourceConfig):
        # Assert that the source_config has a name, schema, and database
        assert source_config.name is not None, "Name is required for loading data!"
        assert source_config.schema is not None, "Schema is required for loading data!"
        assert source_config.database is not None, "Database is required for loading data!"

        table_path = get_storage_location(
            self.uc_client, source_config.name, source_config.schema, source_config.database
        )

        storage_options = source_config.get("storage_options", {})

        dt = DeltaTable(table_path, storage_options=storage_options)

        # delta attributes
        as_of_version = source_config.get("as_of_version", None)
        as_of_datetime = source_config.get("as_of_datetime", None)

        if as_of_version:
            dt.load_version(as_of_version)

        if as_of_datetime:
            dt.load_with_datetime(as_of_datetime)

        df = dt.to_pyarrow_dataset()

        return df

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
        converted_schema = pyarrow_schema_to_columns(schema=df.schema)

        if not uc_schema_exists(self.uc_client, schema_name, self.catalog_name):
            self.uc_client.schemas.create(catalog_name=self.catalog_name, name=schema_name)

        if not uc_table_exists(self.uc_client, table_name, schema_name, self.catalog_name):
            self.uc_client.tables.create(
                catalog_name=self.catalog_name,
                columns=converted_schema,
                data_source_format=storage_format,
                name=table_name,
                schema_name=schema_name,
                table_type="EXTERNAL",
                storage_location=table_path,
            )
        else:
            # TODO: Add support for schema checks/schema evolution with existing schema and dataframe schema
            pass

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
