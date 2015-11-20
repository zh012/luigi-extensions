# -*- coding: utf-8 -*-
import psycopg2
import luigi
from luigi.postgres import PostgresTarget


class ConnectionlMixin(object):
    _dbconf_field_name = 'dbconf'

    def get_dbconf(self):
        return luigi.configuration.get_config().items(getattr(self, self._dbconf_field_name))

    def connect(self):
        dsn = ' '.join('{}={}'.format(k, v) for k, v in self.get_dbconf())
        return psycopg2.connect(dsn)

    def run_sql(self, sql):
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)

    def get_db_target(self, table, update_id, target_cls=PostgresTarget):
        dbconf = dict(self.get_dbconf())
        return target_cls(
            host='{}:{}'.format(dbconf['host'], dbconf['port']),
            database=dbconf['dbname'],
            user=dbconf['user'],
            password=dbconf['password'],
            table=table,
            update_id=update_id
        )


class BaseExecuteSqlTask(luigi.Task):
    def run(self):
        self.run_sql(self.get_sql())
        self._complete = True

    def complete(self):
        return getattr(self, '_complete', False)


class SqlTask(ConnectionlMixin, BaseExecuteSqlTask):
    dbconf = luigi.Parameter()
    sql = luigi.Parameter(default='')
    inline_sql = luigi.Parameter(default='')

    def get_sql(self):
        return (self.inline_sql or open(self.sql, 'r').read())
