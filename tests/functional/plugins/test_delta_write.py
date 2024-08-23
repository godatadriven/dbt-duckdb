import tempfile
from pathlib import Path

import pytest
from dbt.tests.util import (
    run_dbt,
)

ref1 = """
select 2 as a, 'test' as b 
"""


def delta_table_sql(location: str) -> str:
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


@pytest.mark.skip_profile("buenavista", "md")
class TestPlugins:
    @pytest.fixture(scope="class")
    def delta_test_table(self):
        td = tempfile.TemporaryDirectory()
        path = Path(td.name)
        table_path = path / "test_delta_table"

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
    def models(self, delta_test_table):
        return {

            "delta_table.sql": delta_table_sql(str(delta_test_table)),
            "ref1.sql": ref1
        }

    def test_plugins(self, project):
        results = run_dbt()
        assert len(results) == 2
