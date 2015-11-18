# -*- coding: utf-8 -*-
import psycopg2
import luigi


class ConnectionlMixin(object):
    def connect(self):
        dsn = ' '.join('{}={}'.format(k, v) for k, v in luigi.configuration.get_config().items(self.dbconf))
        return psycopg2.connect(dsn)

    def run_sql(self, sql):
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)


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
