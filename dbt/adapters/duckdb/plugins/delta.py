import json
from typing import Any
from typing import Dict

import pyarrow
import pyarrow.compute as pc
from deltalake import DeltaTable
from deltalake import write_deltalake
from unitycatalog import Unitycatalog

from . import BasePlugin
from ..utils import SourceConfig
from ..utils import TargetConfig


class Plugin(BasePlugin):
    CATALOG_NAME: str = "unity"
    CATALOG_BASE_URL: str = "http://unity_catalog:8080/api/2.1/unity-catalog"
    uc_client: Unitycatalog

    def initialize(self, config: Dict[str, Any]):
        self.uc_client: Unitycatalog = Unitycatalog(base_url=self.CATALOG_BASE_URL)

    def configure_cursor(self, cursor):
        pass

    def load(self, source_config: SourceConfig):
        if "delta_table_path" not in source_config:
            raise Exception("'delta_table_path' is a required argument for the delta table!")

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

    @staticmethod
    def convert_schema_to_columns(schema: pyarrow.Schema) -> list:
        def pyarrow_type_to_json_type(data_type: pyarrow.DataType) -> str:
            """Convert PyArrow type to a JSON-compatible type string."""
            if pyarrow.types.is_int8(data_type):
                return "INT"
            elif pyarrow.types.is_int16(data_type):
                return "INT"
            elif pyarrow.types.is_int32(data_type):
                return "INT"
            elif pyarrow.types.is_int64(data_type):
                return "INT"
            elif pyarrow.types.is_float32(data_type):
                return "DOUBLE"
            elif pyarrow.types.is_float64(data_type):
                return "DOUBLE"
            elif pyarrow.types.is_string(data_type):
                return "STRING"
            elif pyarrow.types.is_boolean(data_type):
                return "BOOLEAN"
            elif pyarrow.types.is_decimal(data_type):
                return "DOUBLE"
            elif pyarrow.types.is_date32(data_type):
                return "DATE"
            else:
                raise NotImplementedError(f"Type {data_type} not supported")

        columns = []

        for i, field in enumerate(schema):
            data_type = field.type
            json_type = pyarrow_type_to_json_type(data_type)

            column = {
                "name": field.name,
                "type_name": json_type,
                "nullable": field.nullable,
                "comment": f"Field {field.name}",  # Generic comment, modify as needed
                "position": i,
                "type_interval_type": None,
                "type_json": json.dumps(
                    {
                        "name": field.name,
                        "type": json_type,
                        "nullable": field.nullable,
                        "metadata": field.metadata or {},
                    }
                ),
                "type_precision": 0,
                "type_scale": 0,
                "type_text": json_type,
                "partition_index": None,
            }

            # Adjust type precision and scale for decimal types
            if pyarrow.types.is_decimal(data_type):
                column["type_precision"] = data_type.precision
                column["type_scale"] = data_type.scale

            columns.append(column)

        return columns

    def schema_exists(self, schema_name: str) -> bool:
        """Check if schema exists in the catalog."""
        schemas = [
            schema.name
            for schema in self.uc_client.schemas.list(catalog_name=self.CATALOG_NAME).schemas
        ]
        return schema_name in schemas

    def table_exists(self, table_name: str, schema_name: str = "default") -> bool:
        """Check if table exists in the catalog."""
        tables = [
            table.name
            for table in self.uc_client.tables.list(
                catalog_name=self.CATALOG_NAME, schema_name=schema_name
            ).tables
        ]
        return table_name in tables

    # Future
    # TODO add databricks catalog
    def store(self, target_config: TargetConfig, df: pyarrow.lib.Table = None):
        mode = target_config.config.get("mode", "overwrite")
        table_path = target_config.location.path
        table_name = target_config.relation.identifier
        schema_name = target_config.config.get("schema", "default")
        storage_options = target_config.config.get("storage_options", {})
        converted_schema = self.convert_schema_to_columns(schema=df.schema)

        if not self.schema_exists(schema_name):
            self.uc_client.schemas.create(catalog_name=self.CATALOG_NAME, name=schema_name)

        if not self.table_exists(table_name, schema_name):
            self.uc_client.tables.create(
                catalog_name=self.CATALOG_NAME,
                columns=converted_schema,
                data_source_format="DELTA",
                name=table_name,
                schema_name=schema_name,
                table_type="EXTERNAL",
                storage_location=table_path,
            )

        if mode == "overwrite_partition":
            partition_key = target_config.config.get("partition_key", None)
            if not partition_key:
                raise Exception(
                    "'partition_key' has to be defined when mode 'overwrite_partition'!"
                )

            if isinstance(partition_key, str):
                partition_key = [partition_key]

            partition_dict = []
            for each_key in partition_key:
                unique_key_array = pc.unique(df[each_key])

                if len(unique_key_array) == 1:
                    partition_dict.append((each_key, str(unique_key_array[0])))
                else:
                    raise Exception(
                        f"'{each_key}' column has not one unique value, values are: {str(unique_key_array)}"
                    )
            create_insert_partition(table_path, df, partition_dict, storage_options)
        elif mode == "merge":
            unique_key = target_config.config.get("unique_key", None)
            if not unique_key:
                raise Exception("'unique_key' has to be defined when mode 'merge'!")
            if isinstance(unique_key, str):
                unique_key = [unique_key]

            predicate_stm = " and ".join(
                [
                    f'source."{each_unique_key}" = target."{each_unique_key}"'
                    for each_unique_key in unique_key
                ]
            )

            try:
                target_dt = DeltaTable(table_path, storage_options=storage_options)
            except Exception:
                # TODO handle this better
                write_deltalake(table_or_uri=table_path, data=df, storage_options=storage_options)

            target_dt = DeltaTable(table_path, storage_options=storage_options)
            # TODO there is a problem if the column name is uppercase
            target_dt.merge(
                source=df,
                predicate=predicate_stm,
                source_alias="source",
                target_alias="target",
            ).when_not_matched_insert_all().execute()
        else:
            write_deltalake(
                table_or_uri=table_path,
                data=df,
                mode=mode,
                storage_options=storage_options,
            )


def table_exists(table_path, storage_options):
    # this is bad, i have to find the way to see if there is table behind path
    try:
        DeltaTable(table_path, storage_options=storage_options)
    except Exception:
        return False
    return True


def create_insert_partition(table_path, data, partitions, storage_options):
    """create a new delta table on the path or overwrite existing partition"""

    if table_exists(table_path, storage_options):
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
