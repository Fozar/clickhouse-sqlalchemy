import ast

import sqlalchemy as sa
from sqlalchemy.util import asbool, update_copy

from .utils import FORMAT_SUFFIX
from ... import types
from ..base import ClickHouseDialect, ClickHouseExecutionContextBase
from . import connector


# Export connector version
VERSION = (0, 0, 2, None)


class _HTTPMap(types.Map):
    def bind_expression(self, bindparam):
        return bindparam

    def result_processor(self, dialect, coltype):
        key_processor = self.key_type_impl.dialect_impl(
            dialect
        ).result_processor(dialect, str(self.key_type_impl))
        value_processor = self.value_type_impl.dialect_impl(
            dialect
        ).result_processor(dialect, str(self.value_type_impl))

        def process(value):
            parsed_map = ast.literal_eval(value)
            if not isinstance(parsed_map, dict):
                raise ValueError(
                    "Failed to parse map. Type mismatch: " + parsed_map
                )

            processed_map = {}
            for key, value in parsed_map.items():
                processed_key = (
                    key_processor(key) if key_processor is not None else key
                )
                processed_value = (
                    value_processor(value)
                    if value_processor is not None
                    else value
                )
                processed_map[processed_key] = processed_value

            return processed_map

        return process


class ClickHouseExecutionContext(ClickHouseExecutionContextBase):
    def pre_exec(self):
        # TODO: refactor
        if not self.isinsert and not self.isddl:
            self.statement += ' ' + FORMAT_SUFFIX


class ClickHouseDialect_http(ClickHouseDialect):
    driver = 'http'
    execution_ctx_cls = ClickHouseExecutionContext

    colspecs = update_copy(
        ClickHouseDialect.colspecs,
        {
            types.Map: _HTTPMap,
        },
    )

    @classmethod
    def dbapi(cls):
        return connector

    def create_connect_args(self, url):
        kwargs = {}
        protocol = url.query.get('protocol', 'http')
        port = url.port or 8123
        db_name = url.database or 'default'
        endpoint = url.query.get('endpoint', '')

        self.engine_reflection = asbool(
            url.query.get('engine_reflection', 'true')
        )

        kwargs.update(url.query)
        if kwargs.get('verify') and kwargs['verify'] in ('False', 'false'):
            kwargs['verify'] = False

        db_url = '%s://%s:%d/%s' % (protocol, url.host, port, endpoint)

        return (db_url, db_name, url.username, url.password), kwargs

    def _execute(self, connection, sql, scalar=False, **kwargs):
        if isinstance(sql, str):
            # Makes sure the query will go through the
            # `ClickHouseExecutionContext` logic.
            sql = sa.sql.elements.TextClause(sql)
        f = connection.scalar if scalar else connection.execute
        return f(sql, **kwargs)


dialect = ClickHouseDialect_http
