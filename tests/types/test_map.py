import itertools

from sqlalchemy import Column
from sqlalchemy.sql.ddl import CreateTable

from clickhouse_sqlalchemy import types, engines, Table
from tests.testcase import BaseTestCase, CompilationTestCase
from tests.util import with_native_and_http_sessions, require_server_version


class MapCompilationTestCase(CompilationTestCase):
    supported_key_types = (
        types.String,
        types.Int32,
        types.LowCardinality(types.String),
    )

    supported_values_types = (
        types.String,
        types.Int32,
        types.Array(types.String),
        types.LowCardinality(types.String),
    )

    def generate_tables(self):
        tables = []
        for key_type, value_type in itertools.product(
            self.supported_key_types, self.supported_values_types
        ):
            tables.append(
                Table(
                    "test",
                    CompilationTestCase.metadata(),
                    Column(
                        "x", types.Map(key_type, value_type), primary_key=True
                    ),
                    engines.Memory(),
                )
            )
        return tables

    @require_server_version(21, 1, 3)
    def test_create_table(self):
        for table in self.generate_tables():
            compiled_key_type = str(table.c.x.type.key_type_impl)
            compiled_value_type = str(table.c.x.type.value_type_impl)
            compiled_map_type = (
                f"Map({compiled_key_type}, {compiled_value_type})"
            )
            self.assertEqual(
                self.compile(CreateTable(table)),
                f"CREATE TABLE test (x {compiled_map_type}) ENGINE = Memory",
            )

    @require_server_version(21, 1, 3)
    def test_getitem(self):
        for table in self.generate_tables():
            self.assertEqual(self.compile(table.c.x["a"]), "test.x[%(x_1)s]")


@with_native_and_http_sessions
class MapTestCase(BaseTestCase):
    table = Table(
        "test",
        BaseTestCase.metadata(),
        Column(
            "x", types.Map(types.LowCardinality(types.String), types.Int32)
        ),
        engines.Memory(),
    )

    @require_server_version(21, 1, 3)
    def test_select_insert(self):
        map_ = {"a": 1, "b": 2, "c": 3}

        with self.create_table(self.table):
            self.session.execute(self.table.insert(), [{"x": map_}])
            self.assertEqual(self.session.query(self.table.c.x).scalar(), map_)

    @require_server_version(21, 1, 3)
    def test_select_getitem(self):
        map_ = {"a": 1, "b": 2, "c": 3}

        with self.create_table(self.table):
            self.session.execute(self.table.insert(), [{"x": map_}])
            self.assertEqual(
                self.session.query(self.table.c.x["b"]).scalar(), 2
            )
