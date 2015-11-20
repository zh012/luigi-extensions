# -*- coding: utf-8 -*-
import psycopg2
import luigi
from luigi.postgres import PostgresTarget
from .grocery import ConfiguredObject


class Postgres(ConfiguredObject):
    _dsn_keys = ['host', 'port', 'dbname', 'user', 'password']

    def connect(self):
        dsn = ' '.join('{}={}'.format(k, v) for k, v in self.conf.items() if k in self._dsn_keys)
        return psycopg2.connect(dsn)

    def run_sql(self, sql):
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
        conn.commit()

    def get_target(self, table, update_id, target_cls=PostgresTarget):
        return target_cls(
            host='{}:{}'.format(self.conf['host'], self.conf['port']),
            database=self.conf['dbname'],
            user=self.conf['user'],
            password=self.conf['password'],
            table=table,
            update_id=update_id
        )


class SimpleSqlTask(luigi.Task):
    dbconf = luigi.Parameter()
    sql = luigi.Parameter()

    def complete(self):
        return getattr(self, '_complete', False)

    def get_sql(self):
        try:
            return open(self.sql, 'r').read()
        except:
            return self.sql

    def get_postgres(self):
        return Postgres(self.dbconf)

    def run(self):
        self.get_postgres().run_sql(self.get_sql())
        self._complete = True
