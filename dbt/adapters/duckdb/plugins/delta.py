from __future__ import annotations

import json
from enum import Enum
from typing import Any
from typing import Dict
from typing import Literal

import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable
from deltalake import write_deltalake
from deltalake._internal import TableNotFoundError
from unitycatalog import Unitycatalog
from unitycatalog.types.table_create_params import Column

from . import BasePlugin
from ..utils import SourceConfig
from ..utils import TargetConfig


class WriteModes(str, Enum):
    """Enum class for the write modes supported by the plugin."""

    OVERWRITE_PARTITION = "overwrite_partition"
    MERGE = "merge"
    OVERWRITE = "overwrite"


class PartitionKeyMissingError(Exception):
    """Exception raised when the partition key is missing from the target configuration."""

    pass


class UniqueKeyMissingError(Exception):
    """Exception raised when the unique key is missing from the target configuration."""

    pass


class DeltaTablePathMissingError(Exception):
    """Exception raised when the delta table path is missing from the source configuration."""

    pass


class SecretTypeMissingError(Exception):
    """Exception raised when the secret type is missing from the secrets dictionary."""

    pass


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


def delta_table_exists(table_path: str, storage_options: dict) -> bool:
    """Check if a delta table exists at the given path."""
    try:
        DeltaTable(table_path, storage_options=storage_options)
    except TableNotFoundError:
        return False
    return True


def create_insert_partition(
    table_path: str, data: pa.lib.Table, partitions: list, storage_options: dict
) -> None:
    """Create a new delta table with partitions or overwrite an existing one."""

    if delta_table_exists(table_path, storage_options):
        partition_expr = [
            (partition_name, "=", partition_value)
            for (partition_name, partition_value) in partitions
        ]
        print(
            f"Overwriting delta table under: {table_path} \nwith partition expr: {partition_expr}"
        )
        write_deltalake(table_path, data, partition_filters=partition_expr, mode="overwrite")
    else:
        partitions = [partition_name for (partition_name, partition_value) in partitions]
        print(f"Creating delta table under: {table_path} \nwith partitions: {partitions}")
        write_deltalake(table_path, data, partition_by=partitions)


def find_secrets_by_type(secrets: list[dict], secret_type: str) -> dict:
    """Find secrets of a specific type in the secrets dictionary."""
    for secret in secrets:
        if secret.get("type") == secret_type:
            return secret
    raise SecretTypeMissingError(f"Secret type {secret_type} not found in the secrets!")


class Plugin(BasePlugin):
    # The name of the catalog
    catalog_name: str = "unity"

    # The Unitycatalog client
    uc_client: Unitycatalog | None = None

    def initialize(self, config: Dict[str, Any]):
        if config.get("use_unitycatalog", False):
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

    def configure_cursor(self, cursor):
        pass

    def load(self, source_config: SourceConfig):
        if "delta_table_path" not in source_config:
            raise DeltaTablePathMissingError(
                "'delta_table_path' is a required argument for the delta table!"
            )

        table_path = source_config["delta_table_path"]
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

    def default_materialization(self):
        return "view"

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
        schema_name = target_config.config.get("schema", "default")
        storage_options = target_config.config.get("storage_options", {})
        partition_key = target_config.config.get("partition_key", None)
        unique_key = target_config.config.get("unique_key", None)

        # Convert the pa schema to columns
        converted_schema = pyarrow_schema_to_columns(schema=df.schema)

        if self.uc_client is not None:
            if not uc_schema_exists(self.uc_client, schema_name, self.catalog_name):
                self.uc_client.schemas.create(catalog_name=self.catalog_name, name=schema_name)

            if not uc_table_exists(self.uc_client, table_name, schema_name, self.catalog_name):
                self.uc_client.tables.create(
                    catalog_name=self.catalog_name,
                    columns=converted_schema,
                    data_source_format="DELTA",
                    name=table_name,
                    schema_name=schema_name,
                    table_type="EXTERNAL",
                    storage_location=table_path,
                )
            else:
                # TODO: Add support for schema checks/schema evolution with existing schema and dataframe schema
                pass

        if mode == WriteModes.OVERWRITE_PARTITION:
            if not partition_key:
                raise PartitionKeyMissingError(
                    "'partition_key' has to be defined for mode 'overwrite_partition'!"
                )

            if isinstance(partition_key, str):
                partition_key = [partition_key]

            partition_dict = []
            # TODO: Add support overwriting multiple partitions
            for each_key in partition_key:
                unique_key_array = pc.unique(df[each_key])

                if len(unique_key_array) == 1:
                    partition_dict.append((each_key, str(unique_key_array[0])))
                else:
                    raise Exception(
                        f"'{each_key}' column has not one unique value, values are: {str(unique_key_array)}"
                    )
            create_insert_partition(table_path, df, partition_dict, storage_options)
        elif mode == WriteModes.MERGE:
            if not unique_key:
                raise UniqueKeyMissingError("'unique_key' has to be defined when mode 'merge'!")
            if isinstance(unique_key, str):
                unique_key = [unique_key]

            predicate_stm = " and ".join(
                [
                    f'source."{each_unique_key}" = target."{each_unique_key}"'
                    for each_unique_key in unique_key
                ]
            )

            if not delta_table_exists(table_path, storage_options):
                write_deltalake(table_or_uri=table_path, data=df, storage_options=storage_options)

            target_dt = DeltaTable(table_path, storage_options=storage_options)
            # TODO there is a problem if the column name is uppercase
            target_dt.merge(
                source=df,
                predicate=predicate_stm,
                source_alias="source",
                target_alias="target",
            ).when_not_matched_insert_all().execute()
        elif mode == WriteModes.OVERWRITE:
            write_deltalake(
                table_or_uri=table_path,
                data=df,
                mode="overwrite",
                storage_options=storage_options,
            )
        else:
            raise NotImplementedError(f"Mode {mode} not supported!")

        # TODO: Add support for OPTIMIZE
