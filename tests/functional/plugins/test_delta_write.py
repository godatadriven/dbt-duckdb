import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from dbt.tests.util import (
    run_dbt,
)

delta_schema_yml = """

"""

ref1 = """
select 2 as a, 'test' as b 
"""


def delta1_sql(location: str) -> str:
    return f"""
    {{ config(
        materialized='external_table',
        plugin = 'delta',
        location = '{location}',
        storage_options = {
    'test' : 'test'
        }

    ) }}
    select * from {{ref('ref1')}} 
"""


def delta2_sql(location: str) -> str:
    return f"""
    {{{{ config(
        materialized='external_table',
        plugin = 'delta',
        location = '{location}',
        mode = 'merge',
        unique_key = 'a'

    ) }}}}
    select * from {{{{ref('ref1')}}}}
"""


class MockUnitycatalog:
    def __init__(self, *args, **kwargs):
        # Mock the resources as attributes with MagicMock
        self.catalogs = MagicMock()
        self.schemas = MagicMock()
        self.tables = MagicMock()
        self.volumes = MagicMock()
        self.temporary_table_credentials = MagicMock()
        self.temporary_volume_credentials = MagicMock()
        self.functions = MagicMock()
        self.with_raw_response = MagicMock()
        self.with_streaming_response = MagicMock()

        # Additional mock for methods or properties as needed
        self.qs = MagicMock()
        self.default_headers = MagicMock()

    def copy(self, *args, **kwargs):
        # Mock copy method to return a new instance of MockUnitycatalog
        return MockUnitycatalog()

    def with_options(self, *args, **kwargs):
        # Alias for copy method
        return self.copy()

    def _make_status_error(self, err_msg, *, body, response):
        # Mock for the _make_status_error method
        return MagicMock()


@pytest.mark.skip_profile("buenavista", "md")
class TestPlugins:
    @pytest.fixture(scope="class")
    def delta_test_table1(self):
        td = tempfile.TemporaryDirectory()
        path = Path(td.name)
        table_path = path / "test_delta_table1"

        yield table_path

        td.cleanup()

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        plugins = [{"module": "delta"}]
        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "duckdb",
                        "path": dbt_profile_target.get("path", ":memory:"),
                        "plugins": plugins,
                    }
                },
                "target": "dev",
            }
        }

    @pytest.fixture(scope="class")
    def models(self, delta_test_table1):
        return {

            "delta_table2.sql": delta2_sql(str(delta_test_table1)),
            "ref1.sql": ref1
        }

    @patch("dbt.adapters.duckdb.plugins.delta.Plugin.initialize")
    @patch("dbt.adapters.duckdb.plugins.delta.Plugin.schema_exists")
    @patch("dbt.adapters.duckdb.plugins.delta.Plugin.table_exists")
    def test_plugins(self, mock_table_exists, mock_schema_exists, mock_initialize, project):
        mock_initialize.return_value = None
        mock_schema_exists.return_value = True
        mock_table_exists.return_value = True

        results = run_dbt()
        assert len(results) == 2

        # materializing external view not yet supported
        # check_relations_equal(
        #     project.adapter,
        #     [
        #         "delta_table2",
        #         "delta_table2_expected",
        #     ],
        # )
        # res = project.run_sql("SELECT count(1) FROM 'memory.delta_table2'", fetch="one")
        # assert res[0] == 2
